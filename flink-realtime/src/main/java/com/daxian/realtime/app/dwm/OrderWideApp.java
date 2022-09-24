package com.daxian.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.daxian.realtime.app.func.DimAsyncFunction;
import com.daxian.realtime.beans.OrderDetail;
import com.daxian.realtime.beans.OrderInfo;
import com.daxian.realtime.beans.OrderWide;
import com.daxian.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Author: Felix
 * Date: 2021/8/7
 * Desc: 订单宽表准备
 * -需要启动的进程
 *      zk、kafka、maxwell、hdfs、hbase、redis
 *      BaseDBApp、OrderWideApp
 * -执行流程
 *      >运行模拟生成业务数据的jar
 *      >会向业务数据库MySQL中插入生成业务数据
 *      >MySQL会将变化的数据放到Binlog中
 *      >Maxwell从Binlog中获取数据，将数据封装为json字符串发送到kafka的ods主题  ods_base_db_m
 *      >BaseDBApp从ods_base_db_m主题中读取数据，进行分流
 *          &事实----写回到kafka的dwd主题
 *          &维度----保存到phoenix的维度表中
 *      >OrderWideApp从dwd主题中获取订单和订单明细数据
 *      >使用intervalJoin对订单和订单明细进行双流join
 *      >将用户维度关联到订单宽表上
 *          *基本的维度关联
 *          *优化1：旁路缓存的优化
 *          *优化2：异步IO
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 2.检查点相关设置(略)
        //TODO 3.从Kafka中读取数据
        //3.1 声明消费的主题以及消费者组
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String groupId = "order_wide_app_group";

        //3.2 获取kafka消费者
        //订单
        FlinkKafkaConsumer<String> orderInfoKafkaSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        //订单明细
        FlinkKafkaConsumer<String> orderDetailKafkaSource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);

        //3.3 读取数据  封装为流
        //订单流
        DataStreamSource<String> orderInfoStrDS = env.addSource(orderInfoKafkaSource);
        //订单明细流
        DataStreamSource<String> orderDetailStrDS = env.addSource(orderDetailKafkaSource);

        //TODO 4.对流中数据类型进行转换    String ->实体对象
        //订单
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoStrDS.map(
            new RichMapFunction<String, OrderInfo>() {
                private SimpleDateFormat sdf;

                @Override
                public void open(Configuration parameters) throws Exception {
                    sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                }

                @Override
                public OrderInfo map(String jsonStr) throws Exception {
                    OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                    orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                    return orderInfo;
                }
            }
        );
        //订单明细
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailStrDS.map(
            new RichMapFunction<String, OrderDetail>() {
                private SimpleDateFormat sdf;

                @Override
                public void open(Configuration parameters) throws Exception {
                    sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                }

                @Override
                public OrderDetail map(String jsonStr) throws Exception {
                    OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                    orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                    return orderDetail;
                }
            }
        );

        //orderInfoDS.print(">>>>");
        //orderDetailDS.print("####");

        //TODO 5.指定Watermark并提取事件时间字段
        //订单
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWatermarkDS = orderInfoDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<OrderInfo>() {
                        @Override
                        public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                            return orderInfo.getCreate_ts();
                        }
                    }
                )
        );
        //订单明细
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWatermarkDS = orderDetailDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<OrderDetail>() {
                        @Override
                        public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                            return orderDetail.getCreate_ts();
                        }
                    }
                )
        );

        //TODO 6. 分组  指定两个流的关联字段---------order_id
        //订单
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoWithWatermarkDS.keyBy(OrderInfo::getId);
        //订单明细
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailWithWatermarkDS.keyBy(OrderDetail::getOrder_id);

        //TODO 7.双流join   使用intervalJoin
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS
            .intervalJoin(orderDetailKeyedDS)
            .between(Time.seconds(-5), Time.seconds(5))
            .process(
                new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                }
            );

        //orderWideDS.print(">>>>>");

        //TODO 8.和用户维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
            orderWideDS,
            new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                @Override
                public void join(OrderWide orderWide, JSONObject dimJsonObj) throws ParseException {
                    String gender = dimJsonObj.getString("GENDER");
                    orderWide.setUser_gender(gender);

                    //2005-07-30
                    String birthday = dimJsonObj.getString("BIRTHDAY");
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date birthdayDate = sdf.parse(birthday);
                    Long betweenMs = System.currentTimeMillis() - birthdayDate.getTime();
                    Long ageLong = betweenMs / 1000L / 60L / 60L / 24L / 365L;
                    Integer age = ageLong.intValue();
                    orderWide.setUser_age(age);
                }

                @Override
                public String getKey(OrderWide orderWide) {
                    return orderWide.getUser_id().toString();
                }
            },
            60,
            TimeUnit.SECONDS
        );

        //orderWideWithUserDS.print(">>>>");

        //TODO 9.和地区维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
            orderWideWithUserDS,
            new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                @Override
                public void join(OrderWide orderWide, JSONObject dimJsonObj) throws Exception {
                    orderWide.setProvince_name(dimJsonObj.getString("NAME"));
                    orderWide.setProvince_iso_code(dimJsonObj.getString("ISO_CODE"));
                    orderWide.setProvince_area_code(dimJsonObj.getString("AREA_CODE"));
                    orderWide.setProvince_3166_2_code(dimJsonObj.getString("ISO_3166_2"));
                }

                @Override
                public String getKey(OrderWide orderWide) {
                    return orderWide.getProvince_id().toString();
                }
            },
            60, TimeUnit.SECONDS
        );

        //orderWideWithProvinceDS.print(">>>>");

        //TODO 10.和SKU维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
            orderWideWithProvinceDS,
            new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                @Override
                public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                    orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                    orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                    orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                    orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                }

                @Override
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getSku_id());
                }
            }, 60, TimeUnit.SECONDS);

        //TODO 11.和SPU维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
            orderWideWithSkuDS,
            new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                @Override
                public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                    orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                }

                @Override
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getSpu_id());
                }
            }, 60, TimeUnit.SECONDS);

        //TODO 12.和类别度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
            orderWideWithSpuDS,
            new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                @Override
                public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                    orderWide.setCategory3_name(jsonObject.getString("NAME"));
                }

                @Override
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getCategory3_id());
                }
            }, 60, TimeUnit.SECONDS);

        //TODO 13.和品牌维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
            orderWideWithCategory3DS,
            new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                @Override
                public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                    orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                }

                @Override
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getTm_id());
                }
            }, 60, TimeUnit.SECONDS);


        orderWideWithTmDS.print(">>>>>");

        //TODO 14.将订单宽表数据写回到kafka的dwm_order_wide
        //JSON.parseObject(jsonStr)         :将json格式字符串转换为json对象
        //JSON.parseObject(jsonStr,类型)     :将json格式字符串转换为指定类型对象
        //JSON.toJSONString(orderWide)      :将对象转换为json格式字符串
        orderWideWithTmDS
            .map(orderWide->JSON.toJSONString(orderWide))
            .addSink(
                MyKafkaUtil.getKafkaSink("dwm_order_wide")
        );
        env.execute();
    }
}

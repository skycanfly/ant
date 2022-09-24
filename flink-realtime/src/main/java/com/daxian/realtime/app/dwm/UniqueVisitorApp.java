package com.daxian.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.daxian.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

/**
 * Author: Felix
 * Date: 2021/8/6
 * Desc: 独立访客计算
 * 需要启动的进程
 *  zk、kafka、模拟日志jar、logger.sh、UniqueVisitorApp
 * 执行流程
 *  -运行模拟生成日志的jar
 *  -将模拟生成的日志数据发给nginx进行负载均衡
 *  -nginx将请求转发给三台日志采集服务
 *  -三台日志采集服务接收到日志数据  将日志数据发送到kafka的ods_base_log主题中
 *  -BaseLogApp应用程序从ods_base_log中读取数据，进行分流
 *      >启动日志   ---dwd_start_log
 *      >曝光日志   ---dwd_display_log
 *      >页面日志   ---dwd_page_log
 *  -UniqueVisitorApp从dwd_page_log主题读取
 *  -对pv数据进行过滤
 *      >按照mid进行分组
 *      >使用filter算子对数据进行过滤
 *      >在过滤的时候，使用状态变量记录上次访问日期
 *
 */
public class UniqueVisitorApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        /*//TODO 2.检查点相关设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //2.3 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //2.4 设置job取消后，检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.5 设置状态后端   内存|文件系统|RocksDB
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/ck/gmall"));
        //2.6 指定操作HDFS的用户
        System.setProperty("HADOOP_USER_NAME","daxian");*/

        //TODO 3.从kafka中读取数据
        //3.1 声明消费主题以及消费者组
        String topic = "dwd_page_log";
        String groupId = "unique_visitor_app_group";
        //3.2 获取kafka消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //3.3 读取数据封装流
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //TODO 4.对读取的数据进行类型转换 String ->JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 5.按照设备id对数据进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 6.过滤实现
        SingleOutputStreamOperator<JSONObject> filterDS = keyedDS.filter(
            new RichFilterFunction<JSONObject>() {
                //声明状态变量，用于存放上次访问日期
                private ValueState<String> lastVisitDateState;
                //转换日期格式工具类
                private SimpleDateFormat sdf;

                @Override
                public void open(Configuration parameters) throws Exception {
                    sdf = new SimpleDateFormat("yyyyMMdd");
                    ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                    //注意：UV其实可以延伸为日活统计，如果是日活的话，状态值主要用于筛选当天是否访问过，所以状态过了今天基本上就没有存在的意义了
                    //所以我们这类设置状态的失效时间为1天
                    StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1))
                        //默认值   当状态创建或者写入的时候更新失效时间
                        //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        //默认值   状态过期后，如果还没有被清理，是否返回给状态调用者
                        //.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();
                    valueStateDescriptor.enableTimeToLive(ttlConfig);
                    lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                }

                @Override
                public boolean filter(JSONObject jsonObj) throws Exception {
                    //如果是从其他页面跳转过来的  直接过滤掉
                    String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                    if (lastPageId != null && lastPageId.length() > 0) {
                        return false;
                    }

                    //获取状态中的上次访问日志
                    String lastVisitDate = lastVisitDateState.value();
                    String curVisitDate = sdf.format(jsonObj.getLong("ts"));

                    if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(curVisitDate)) {
                        //已经访问过
                        return false;
                    } else {
                        //还没有访问过
                        lastVisitDateState.update(curVisitDate);
                        return true;
                    }
                }
            }
        );
        filterDS.print(">>>>");

        //TODO 7.将过滤后的UV数据 写回到kafka的dwm层
        filterDS.map(jsonObj->jsonObj.toJSONString()).addSink(
            MyKafkaUtil.getKafkaSink("dwm_unique_visitor")
        );

        env.execute();
    }
}

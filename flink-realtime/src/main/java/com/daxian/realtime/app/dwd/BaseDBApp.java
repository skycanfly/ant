package com.daxian.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.daxian.realtime.app.func.TableProcessFunction;
import com.daxian.realtime.beans.TableProcess;
import com.daxian.realtime.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * Author: daxian
 * Date: 2021/12/31
 * Desc: 业务数据动态分流
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);

        /*
        //TODO 2.检查点设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //2.4 设置job取消后，检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.5 设置状态后端   内存|文件系统|RocksDB
        env.setStateBackend(new FsStateBackend("xxx"));
        //2.6 指定操作HDFS的用户
        System.setProperty("HADOOP_USER_NAME","daxian");
        */
        //TODO 3.从Kafka中读取数据
        //3.1 声明消费主题以及消费者组
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";

        //3.2 获取消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);

        //3.3 读取数据  封装流
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //TODO 4.对数据类型进行转换  String->JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 5.简单的ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
            new FilterFunction<JSONObject>() {
                @Override
                public boolean filter(JSONObject jsonObj) throws Exception {
                    boolean flag =
                        jsonObj.getString("table") != null
                            && jsonObj.getString("table").length() > 0
                            && jsonObj.getJSONObject("data") != null
                            && jsonObj.getString("data").length() > 3;
                    return flag;
                }
            }
        );

        //filterDS.print(">>>>");

        //TODO 6. 使用FlinkCDC读取配置表数据
        //6.1 获取sourceFunction
        MySqlSource<String> mySQLSourceFunction = MySqlSource.<String>builder()
            .hostname("localhost")
            .port(3306)
            .databaseList("daxian")
            .tableList("daxian.table_process")
            .username("root")
            .password("123456")
            .startupOptions(StartupOptions.initial())
            .deserializer(new JsonDebeziumDeserializationSchema())
            .build();

        //6.2 读取数据封装流
        DataStreamSource<String> mySQLDS = env.fromSource(mySQLSourceFunction, WatermarkStrategy.noWatermarks(), "MySQL Source");

        //6.3 为了让每一个并行度上处理业务数据的时候  都能使用配置流的数据，那么需要将配置流广播下去
        MapStateDescriptor<String, TableProcess>
            mapStateDescriptor = new MapStateDescriptor<>("table_process", String.class, TableProcess.class);

        BroadcastStream<String> broadcastDS = mySQLDS.broadcast(mapStateDescriptor);

        //6.4 调用非广播流的connect方法  将业务流与配置流进行连接
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastDS);

        //TODO 7. 动态分流  将维度数据放到维度侧输出流       实时数据放到主流
        //声明维度侧输出流的标记
        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dimTag"){};
        SingleOutputStreamOperator<JSONObject> realDS = connectDS.process(
            new TableProcessFunction(dimTag,mapStateDescriptor)
        );
        //获取维度侧输出流
        DataStream<JSONObject> dimDS = realDS.getSideOutput(dimTag);
        realDS.print(">>>");
        dimDS.print("###");

        //TODO 8.将维度侧输出流的数据写到Hbase中
       // dimDS.addSink(new DimSink());

        //TODO 9.将主流数据写回到Kafka的dwd层
        realDS.addSink(
            MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                    String topic = jsonObj.getString("sink_table");
                    return new ProducerRecord<byte[], byte[]>(topic,jsonObj.getJSONObject("data").toJSONString().getBytes());
                }
            })
        );

        env.execute();
    }
}

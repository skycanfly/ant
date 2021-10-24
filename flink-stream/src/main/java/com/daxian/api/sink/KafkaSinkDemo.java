package com.daxian.api.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Author: daxian
 * @Date: 2021/10/24 8:52 下午
 */
public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");

        DataStreamSink<String> uv = env
                .readTextFile("/Users/mac/code/ant/flink-stream/src/main/resources/UserBehavior.csv")
                .addSink(new FlinkKafkaProducer<String>(
                        "uv",
                        new SimpleStringSchema(),
                        properties
                ));
        //转化


        env.execute();
    }
}

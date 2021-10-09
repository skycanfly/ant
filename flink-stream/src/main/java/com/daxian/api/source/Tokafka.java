package com.daxian.api.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * @Author: daxian
 * @Date: 2021/10/1 11:26 下午
 */
public class Tokafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty(
                "key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
        );
        properties.setProperty(
                "value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
        );
        properties.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>("", new SimpleStringSchema(), properties);
        DataStream<String> stream = env
                // source为来自Kafka的数据，这里我们实例化一个消费者，topic为hotitems
                .addSource(stringFlinkKafkaConsumer);
    }
}

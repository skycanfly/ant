package com.daxian.kafka;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class kafkaExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("bootstrap.servers", "192.168.80.138:9092");
        properties.setProperty("group.id", "test");
        FlinkKafkaProducer<String> woshi = new FlinkKafkaProducer<String>("test", new Serialion("test"), properties, Semantic.EXACTLY_ONCE);
        DataStreamSource<String> ds = env.fromElements("AA", "BB", "CC", "DD", "EE");
        ds.addSink(woshi);
        env.execute("daxian");
    }

    static class Serialion implements KafkaSerializationSchema<String> {
        private String topic;

        public Serialion(String topic) {
            super();
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
            return new ProducerRecord<byte[], byte[]>(topic, element.getBytes(StandardCharsets.UTF_8));
        }
    }
}

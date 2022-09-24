package com.shixutu.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Author: daxian
 * @Date: 2021/11/19 9:42 上午
 */
public class KafkaDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.3.168:9092,192.168.3.171:9092,192.168.3.172:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for(int i = 0; i < 100; i++){
            producer.send(new ProducerRecord<String, String>("daxiana","woshiceshi"+i));
            System.out.println(1111);
            producer.flush();

        }
producer.close();
    }
}

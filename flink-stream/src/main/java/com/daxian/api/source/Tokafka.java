package com.daxian.api.source;

import com.daxian.api.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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
        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>("uv", new SimpleStringSchema(), properties);
        DataStream<String> stream = env
                // source为来自Kafka的数据，这里我们实例化一个消费者，topic为hotitems
                .addSource(stringFlinkKafkaConsumer);

        stream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] arr = s.split(",");
                return new UserBehavior(
                        arr[0], arr[1], arr[2], arr[3],
                        Long.parseLong(arr[4]) * 1000L
                );
            }
        })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .keyBy(r -> true)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .process(new ProcessWindowFunction<UserBehavior, String, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean aBoolean, Context context, Iterable<UserBehavior> elements, Collector<String> out) throws Exception {
                        HashMap<String, Long> hashMap = new HashMap<>();
                        for (UserBehavior e : elements) {
                            if (hashMap.containsKey(e.itemId)) {
                                hashMap.put(e.itemId, hashMap.get(e.itemId) + 1L);
                            } else {
                                hashMap.put(e.itemId, 1L);
                            }
                        }

                        ArrayList<Tuple2<String, Long>> arrayList = new ArrayList<>();
                        for (String key : hashMap.keySet()) {
                            arrayList.add(Tuple2.of(key, hashMap.get(key)));
                        }

                        arrayList.sort(new Comparator<Tuple2<String, Long>>() {
                            @Override
                            public int compare(Tuple2<String, Long> t2, Tuple2<String, Long> t1) {
                                return t1.f1.intValue() - t2.f1.intValue();
                            }
                        });

                        StringBuilder result = new StringBuilder();
                        result
                                .append("========================================\n")
                                .append("窗口：" + new Timestamp(context.window().getStart()) + "~" + new Timestamp(context.window().getEnd()))
                                .append("\n");
                        for (int i = 0; i < 3; i++) {
                            Tuple2<String, Long> currElement = arrayList.get(i);
                            result
                                    .append("第" + (i+1) + "名的商品ID是：" + currElement.f0 + "; 浏览次数是：" + currElement.f1)
                                    .append("\n");
                        }
                        out.collect(result.toString());
                    }
                })
                .print();
        ;

    }
}

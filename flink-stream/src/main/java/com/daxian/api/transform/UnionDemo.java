package com.daxian.api.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: daxian
 * @Date: 2021/10/19 10:25 下午
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2);

        DataStreamSource<Integer> stream2 = env.fromElements(3, 4);

        DataStreamSource<Integer> stream3 = env.fromElements(5, 6);

        DataStream<Integer> result = stream1.union(stream2, stream3);

        result.print();

        env.execute();
    }
}

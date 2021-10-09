package com.daxian.api.transform;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: daxian
 * @Date: 2021/10/3 9:27 下午
 */
public class SumFunctioDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<Integer, Integer>> stream = env
                .fromElements(
                        Tuple2.of(1, 2),
                        Tuple2.of(1, 3)
                );

        KeyedStream<Tuple2<Integer, Integer>, Integer> keyedStream = stream.keyBy(r -> r.f0);

        keyedStream.sum(1).print();


        keyedStream.reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> t1) throws Exception {
                return  Tuple2.of(integerIntegerTuple2.f0,Math.max(integerIntegerTuple2.f1,t1.f1));
            }
        }).print();
        env.execute();
    }
}

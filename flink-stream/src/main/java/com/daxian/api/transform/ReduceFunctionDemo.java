package com.daxian.api.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Author: daxian
 * @Date: 2021/10/3 9:54 下午
 */
public class ReduceFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ds = env.addSource(new SourceFunction<Integer>() {
            private Boolean isRunning = true;
            private Random random = new Random();

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                while (isRunning) {
                    sourceContext.collect(random.nextInt(10));
                }

            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        ds.map(x-> Tuple2.of(x,1))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy(x->true)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> t1) throws Exception {
                        return Tuple2.of(integerIntegerTuple2.f0+t1.f0,integerIntegerTuple2.f1+t1.f1);
                    }
                })
                .map(new MapFunction<Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public Double map(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                        return  (double) integerIntegerTuple2.f0/integerIntegerTuple2.f1;
                    }
                }).print();
        env.execute();
    }
}

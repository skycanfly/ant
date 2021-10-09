package com.daxian.api.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Author:   函数的三种实现方式：
 * lambda（会擦除边界），内部类，自定义实现类
 * @Date: 2021/10/3 9:16 下午
 */
public class MapFunctionDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    private Random random = new Random();
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while (running) {
                            ctx.collect(random.nextInt(1000));
                            Thread.sleep(1000);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .map(r -> Tuple2.of(r, r))
                // 会被擦除成Tuple2<Object, Object>
                // 需要returns方法来标注一下map函数的输出类型
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .print();
        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    private Random random = new Random();
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while (running) {
                            ctx.collect(random.nextInt(1000));
                            Thread.sleep(1000);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Integer value) throws Exception {
                        return Tuple2.of(value, value);
                    }
                })
                .print();
        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    private Random random = new Random();
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while (running) {
                            ctx.collect(random.nextInt(1000));
                            Thread.sleep(1000);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .map(new MyMap())
                .print();

    }
    public static class MyMap implements MapFunction<Integer, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Integer value) throws Exception {
            return Tuple2.of(value, value);
        }
    }
}

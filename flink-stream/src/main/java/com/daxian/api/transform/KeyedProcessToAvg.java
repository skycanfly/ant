package com.daxian.api.transform;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Random;


/**
 * @Author: daxian
 * /使用KeyedProcessFunction 实现一个平均数字的聚合
 * @Date: 2021/10/3 10:41 下午
 */
public class KeyedProcessToAvg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean flage = true;
                    private Random random = new Random();

                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        while (flage) {
                            int nb = random.nextInt(10);
                            System.out.println("发出的数字为 "+nb);
                            sourceContext.collect(nb);
                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {
                        flage = false;
                    }
                })
                .keyBy(r -> 1)
                .process(new KeyedProcessFunction<Integer, Integer, Double>() {
                    //存储sum，和个数
                    private ValueState<Tuple2<Integer,Integer>> valueState;
                    private ValueState<Long> ts;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        valueState= getRuntimeContext().getState(
                                new ValueStateDescriptor<Tuple2<Integer, Integer>>("sum-avg", Types.TUPLE(Types.INT, Types.INT))
                        );
                        ts=getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-avg",Types.LONG));

                    }


                    @Override
                    public void processElement(Integer integer, Context context, Collector<Double> collector) throws Exception {
                         if(valueState.value()==null){
                            valueState.update(Tuple2.of(integer,1));
                         }else {
                             Tuple2<Integer, Integer> value = valueState.value();
                             valueState.update(Tuple2.of(value.f0+integer,value.f1+1));
                         }

                        //collector.collect((double) valueState.value().f0 / valueState.value().f1);
                        if (ts.value() == null) {
                            long tenSecLater = context.timerService().currentProcessingTime() + 5 * 1000L;
                            context.timerService().registerProcessingTimeTimer(tenSecLater);
                            ts.update(tenSecLater);
                        }
                    }
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Double> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        if (valueState.value() != null) {
                            out.collect((double) valueState.value().f0 / valueState.value().f1);
                            ts.clear();
                        }
                    }
                })
                .print();

        env.execute();
    }


}

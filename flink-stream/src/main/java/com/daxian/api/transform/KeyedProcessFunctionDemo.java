package com.daxian.api.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;


/**
 * @Author: daxian
 * @Date: 2021/10/3 10:41 下午
 */
public class KeyedProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("localhost", 9999)
                .keyBy(r -> 1)
                .process(new MyKeyed())
                .print();

        env.execute();
    }

    private static class MyKeyed extends KeyedProcessFunction<Integer, String, String> {

        @Override
        public void processElement(String s, Context context, Collector<String> collector) throws Exception {
            //获取 当前机器时间
            long l = context.timerService().currentProcessingTime();
            collector.collect(s);
            // 注册一个10秒钟之后的定时器
              long ts=l+10*1000l;
            // 注册定时器的语法，注意：注册的是处理时间（机器时间）
            context.timerService().registerProcessingTimeTimer(ts);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("定时器触发了！触发时间是：" + new Timestamp(timestamp));
        }
    }
}

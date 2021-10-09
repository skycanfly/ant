package com.daxian.api.transform;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @Author: daxian
 * @Date: 2021/10/4 12:21 上午
 */
public class TemperatureUpDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    private Random random = new Random();

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while (running) {
                            ctx.collect(random.nextInt());
                            Thread.sleep(300L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .keyBy(r -> 1)
                .process(new IntIncreaseAlert())
                .print();

        env.execute();
    }

    private static class IntIncreaseAlert extends KeyedProcessFunction<Integer, Integer, String> {
        private ValueState<Integer> lasInt;
        private ValueState<Long> timeTs;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lasInt = getRuntimeContext().getState(
                    new ValueStateDescriptor<Integer>("up", Types.INT)
            );
            timeTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("tsa", Types.LONG)
            );
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("整数连续1s上升了！");
            timeTs.clear();
        }

        /**
         * 第一个元素进来，保存状态，
         * 第二个抓元素进来且大于第一个，则定义触发器，10s后出发
         *
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
            /**
             * 取出上次的数据，保存新的数据
             */
            Integer preInt = null;
            if (lasInt.value() != null) {
                preInt = lasInt.value();
            }
            lasInt.update(value);

            /**
             * 取出时间节点数据
             */
            Long ts = null;
            if (timeTs.value()!= null) {
                ts= timeTs.value();
            }
            /**
             * 数字下降，则删除定时器，清空状态
             */
            if (preInt==null||value<preInt) {
                if(ts!=null){
                    ctx.timerService().deleteEventTimeTimer(ts);
                    timeTs.clear();
                }
            }else if ( value > preInt && ts == null) {
                long oneSecLater = ctx.timerService().currentProcessingTime() + 1000L;
                ctx.timerService().registerProcessingTimeTimer(oneSecLater);
                timeTs.update(oneSecLater);
            }

        }
    }
}

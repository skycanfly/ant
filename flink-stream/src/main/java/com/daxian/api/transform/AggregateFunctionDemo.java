package com.daxian.api.transform;

import com.daxian.api.bean.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Calendar;
import java.util.Random;

/**
 * @Author: daxian
 * @Date: 2021/10/10 9:35 下午
 */
public class AggregateFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.getName())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new CountAgg())
                .print();

        env.execute();
    }
    // SourceFunction并行度只能为1
    // 自定义并行化版本的数据源，需要使用ParallelSourceFunction
    public static class ClickSource implements SourceFunction<Event> {
        private boolean running = true;
        private String[] userArr = {"Mary", "Bob", "Alice", "Liz"};
        private String[] urlArr = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        private Random random = new Random();
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            while (running) {
                // collect方法，向下游发送数据
                ctx.collect(
                        new Event(
                                userArr[random.nextInt(userArr.length)],
                                urlArr[random.nextInt(urlArr.length)],
                                Calendar.getInstance().getTimeInMillis()
                        )
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * AggregateFunction 输入，累加器，输出
     */
    public static class CountAgg implements AggregateFunction<Event, Integer, Integer> {
        // 创建累加器
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        // 定义累加规则
        @Override
        public Integer add(Event value, Integer accumulator) {
            return accumulator + 1;
        }

        // 在窗口关闭时返回结果
        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a+b;
        }
    }

}

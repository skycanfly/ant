package com.daxian.api.wc;

import com.daxian.api.bean.UserBehavior;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @Author: daxian
 * 每个窗口最热门的商品
 * 1/按照窗口分组
 * 2/获取窗口内的数据
 * <p>
 * 1/读取数据
 * 2/绑定eventtime +乱序
 * 3/
 * @Date: 2021/10/17 11:38 下午
 */
public class WindowTop {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("/Users/mac/code/ant/flink-stream/src/main/resources/UserBehavior.csv");

        /**
         * 加载数据
         * 过滤出pv
         * 绑定时间+乱序
         * 使用商品id进行分流，所有同类的商品在一个窗口内
         * 窗口设定周期间隔
         * 增量+全量窗口聚合，每个商品在每个窗口中的浏览次数
         * 根据窗口排序
         * 获取窗口内的前三名
         */
        stringDataStreamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] split = s.split(",");

                return new UserBehavior(split[0],
                        split[1],
                        split[2],
                        split[3],
                        Long.parseLong(split[4]) * 1000L);
            }
        }).
                filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior userBehavior, long l) {
                                        return userBehavior.timestamp;
                                    }
                                })
                )
                .keyBy(r -> r.itemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(x -> x.windowEnd)
                .process(new Topn(3))
                .print()
        ;
        env.execute();
    }

    //K, I, O
    private static class Topn extends KeyedProcessFunction<Long, ItemViewCount, String> {
        private ListState<ItemViewCount> listState;
        private long n;

        public Topn(long n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCount>("list-state", Types.POJO(ItemViewCount.class))
            );
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            super.onTimer(timestamp, ctx, out);
            ArrayList<ItemViewCount> itemViewCounts = new ArrayList<>();
            for (ItemViewCount ivc : listState.get()) itemViewCounts.add(ivc);
            listState.clear();
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });
            // 将排名信息格式化成String，方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
            // 遍历列表，取top n输出
            for (int i = 0; i < Math.min(n, itemViewCounts.size()); i++) {
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);
            out.collect(resultBuilder.toString());
        }

    }

    private static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    //IN, OUT, KEY, W extends Window
    private static class WindowResult extends ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<ItemViewCount> out) throws Exception {
            out.collect(new ItemViewCount(s, elements.iterator().next(), context.window().getStart(), context.window().getEnd()));
        }
    }

    // 每个商品在每个窗口中的浏览次数
    public static class ItemViewCount {
        public String itemId;
        public Long count;
        public Long windowStart;
        public Long windowEnd;

        public ItemViewCount() {
        }

        public String getItemId() {
            return itemId;
        }

        public void setItemId(String itemId) {
            this.itemId = itemId;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        public Long getWindowStart() {
            return windowStart;
        }

        public void setWindowStart(Long windowStart) {
            this.windowStart = windowStart;
        }

        public Long getWindowEnd() {
            return windowEnd;
        }

        public void setWindowEnd(Long windowEnd) {
            this.windowEnd = windowEnd;
        }

        public ItemViewCount(String itemId, Long count, Long windowStart, Long windowEnd) {
            this.itemId = itemId;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "ItemViewCount{" +
                    "itemId='" + itemId + '\'' +
                    ", count=" + count +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    '}';
        }
    }


}


package com.daxian.api.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.util.Arrays;

/**
 * @Author: daxian
 * 读取流式数据
 * @Date: 2021/8/21 8:14 下午
 */
public class WordCountStream {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        /**
         * 设置运行环境
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 读取nc数据
         */
//       DataStreamSource<String> ds = env.socketTextStream(parameterTool.get("host"),
//               parameterTool.getInt("port"));
        DataStreamSource<String> ds=  env.fromElements("hello world","hello daxian","hello ceshi","hello tas");
      //  DataStreamSource<String> ds = env.readTextFile("/Users/mac/code/ant/flink-stream/src/main/resources/wc.txt");
        /**
         * 拆分nc输入的字符，
         */
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = ds.flatMap(new Splitter());
        /**
         *  空格分词打散之后，对单词进行 groupby 分组，然后用 sum 进行聚合
         */
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = streamOperator.keyBy(x -> x.f0);
        tuple2StringKeyedStream.print();
        /**
         * window窗口为滚动触发
         */
//        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = tuple2StringKeyedStream.window(
//                TumblingProcessingTimeWindows.of(Time.seconds(5)));
        /**
         * 然后用 sum 进行聚合
         */
        DataStream sm  = tuple2StringKeyedStream.sum(1);
        sm.print();
//               env
//                .socketTextStream("localhost", 6666)
//                .flatMap(new Splitter())
//                .keyBy(a -> a.f0)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .sum(1).print();

        env.execute("Window WordCount");

    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}


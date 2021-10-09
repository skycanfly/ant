package com.daxian.api.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @Author: daxian
 *  批处理读取文件
 * @Date: 2021/8/29 10:53 下午
 */
public class WordCoutBatch {
    public static void main(String[] args) throws Exception {
      //  ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> st = env.readTextFile("/Users/mac/code/ant/flink-stream/src/main/resources/wc.txt");
//        AggregateOperator<Tuple2<String, Integer>> sum = st.flatMap(
//                new FlatMapFunction<String, Tuple2<String, Integer>>() {
//                    @Override
//                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                        for (String word : s.split(" ")) {
//                            collector.collect(new Tuple2<String, Integer>(word, 1));
//                        }
//                    }
//                }).groupBy(0).sum(1);
//        sum.print();
//         st.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
//             @Override
//             public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                 for (String word : s.split(" ")) {
//                            collector.collect(new Tuple2<String, Integer>(word, 1));
//                        }
//             }
//         }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
//             @Override
//             public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                 return stringIntegerTuple2.f0;
//             }
//         }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//             @Override
//             public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
//                 return  new Tuple2<>(
//                         stringIntegerTuple2.f0,stringIntegerTuple2.f1+t1.f1
//                 );
//             }
//         }).print();
         env.execute();
    }
}

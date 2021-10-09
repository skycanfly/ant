package com.daxian.api.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: daxian
 * @Date: 2021/10/3 9:23 下午
 */
public class FlatMapFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.fromElements("white", "black", "gray");
          stream.flatMap(new FlatMapFunction<String, String>() {
              @Override
              public void flatMap(String s, Collector<String> collector) throws Exception {
                  if(s.equalsIgnoreCase("white")){
                      collector.collect("1");
                  }else {
                      collector.collect("2");
                  }
              }
          }).print();
          env.execute();
    }
}

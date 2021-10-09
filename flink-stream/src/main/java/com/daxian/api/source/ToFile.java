package com.daxian.api.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: daxian
 * @Date: 2021/10/1 4:58 下午
 */
public class ToFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("/Users/mac/code/ant/flink-stream/src/main/resources/wc.txt");
        stringDataStreamSource.print();
        env.execute();
    }
}

package com.daxian.api.source;

import com.daxian.api.bean.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Timestamp;
import java.util.Random;

/**
 * @Author: daxian
 * @Date: 2021/10/2 11:11 下午
 */
public class MockData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new ClickSource()).print();
        env.execute();

    }

    public static class ClickSource implements SourceFunction<Event> {
        private Boolean flag = true;
        private String[] userArr = {"daxian", "ceshi", "tiantian", "xaner"};
        private String[] urlArr = {"./daxian", "/home", "./click?id=1", "./produce"};
        private Random ra = new Random();

        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            while (flag) {
                sourceContext.collect(
                        new Event(userArr[ra.nextInt(userArr.length)], urlArr[ra.nextInt(urlArr.length)],
                                System.currentTimeMillis())
                );
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }


}


package com.daxian.api.source;

import com.daxian.api.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;

/**
 * @Author: daxian
 * @Date: 2021/8/29 10:53 下午
 */
public class ToCollect {
    public static void main(String[] args) throws Exception {
        long l = LocalDateTime.now().toEpochSecond(ZoneOffset.of("+8"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * to aslist
         */
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.fromCollection(
                Arrays.asList(
                new SensorReading("name_1", l + 10, 35.6),
                new SensorReading("name_2", l + 20, 16.8),
                new SensorReading("name_3", l + 30, 5.2),
                new SensorReading("name_3", l + 30, 33.1)
        ));
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6);
        integerDataStreamSource.print();
        sensorReadingDataStreamSource.print();


        env.execute();
    }
}

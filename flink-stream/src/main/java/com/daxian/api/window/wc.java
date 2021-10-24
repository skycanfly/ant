package com.daxian.api.window;

import com.daxian.api.bean.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author: daxian
 * 使用flatmap 传感器问相差报警
 * @Date: 2021/8/21 8:15 下午
 */
public class wc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> localhost = env.socketTextStream("localhost", 7777);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<SensorReading> map = localhost.map(x -> {
            String[] fields = x.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        map.keyBy(x -> x.getId())
                .flatMap(new flat(10.0));

        // map.print();
        env.execute();

    }

    public static class flat extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        private Double tmp;
        private ValueState<Double> valueState;

        public flat(Double tmp) {
            this.tmp = tmp;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState= getRuntimeContext().getState(new ValueStateDescriptor<Double>("tmp",Double.class));
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            Double value = valueState.value();
            if(value!=null){
                double max = Math.max(value, sensorReading.getTemperature());
                if(max>10){
                    collector.collect(Tuple3.of("温度大于10",value,sensorReading.getTemperature()));
                }
            }
            valueState.update(sensorReading.getTemperature());
        }


    }

}


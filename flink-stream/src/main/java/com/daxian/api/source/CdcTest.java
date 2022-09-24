package com.daxian.api.source;


import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * @Author: daxian
 * @Date: 2022/3/20 8:00 下午
 */
public class CdcTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();
        env.readCsvFile("")
                .fieldDelimiter("");

    }

    private static void parameterstTest() throws Exception {
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Integer> integerDataSource = env.fromElements(1, 2, 3, 5, 10, 12, 15, 16);
        Configuration configuration=new Configuration();
        configuration.setInteger("limit",8);
        integerDataSource.filter(new RichFilterFunction<Integer>() {
            private  Integer limit;
            @Override
            public void open(Configuration parameters) throws Exception {
                limit=parameters.getInteger("limit",0);
            }

            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer>limit;
            }
        }).withParameters(configuration).print();
    }

}

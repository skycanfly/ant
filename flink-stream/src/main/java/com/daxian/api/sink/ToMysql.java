package com.daxian.api.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import scala.reflect.internal.Types;

import java.util.Random;

/**
 * @Author: daxian
 * @Date: 2021/10/4 2:43 下午
 */
public class ToMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> integerDataStreamSource = env.addSource(new SourceFunction<Integer>() {
            private Boolean run = true;
            private Random random = new Random();

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                while (run) {
                    sourceContext.collect(random.nextInt(10));
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {
                run = false;
            }
        });


        // 执行插入操作
        JdbcStatementBuilder builder=  (JdbcStatementBuilder<Integer>) (ps, t) -> {
            ps.setInt(1, t);
            ps.setString(2, "daxian" + t);
            ps.setInt(3, t);
        };

        // 执行配置
        JdbcExecutionOptions executionOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(1) // 测试，所以设置为1
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        // 连接配置
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions
                .JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://localhost:3306/test?useSSL=false") // url
                .withDriverName("com.mysql.jdbc.Driver") // driver
                .withUsername("root") // 账号
                .withPassword("123456") // 密码
                .withConnectionCheckTimeoutSeconds(30) // 连接超时时间 单位：秒
                .build();
        String insertSql =   "insert into person (id,name,age) values (?,?,?)";


        SinkFunction<Integer> sink = JdbcSink.sink(insertSql,builder,executionOptions,connectionOptions);

        integerDataStreamSource.addSink(sink);
        integerDataStreamSource.print();
        env.execute();

    }
}

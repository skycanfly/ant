package com.daxian.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: daxian
 * @Date: 2021/11/29 3:25 下午
 */
public class Sql {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.3 设置并行度
        env.setParallelism(1);

        //TODO 2.转换动态表
        tableEnv.executeSql("CREATE TABLE user_info (" +
                " id INT NOT NULL," +
                " name STRING," +
                " age INT" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'hadoop202'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '123456'," +
                " 'database-name' = 'gmall0224_realtime'," +
                " 'table-name' = 't_user'" +
                ")");

        tableEnv.executeSql("select * from user_info").print();

        env.execute();

    }
}

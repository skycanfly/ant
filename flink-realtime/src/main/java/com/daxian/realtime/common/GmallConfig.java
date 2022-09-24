package com.daxian.realtime.common;

/**
 * Author: Felix
 * Date: 2021/8/5
 * Desc: 实时数仓项目的常量类
 */
public class GmallConfig {
    public static final String HBASE_SCHEMA = "test";
    public static final String PHOENIX_SERVER = "jdbc:phoenix:controller,compute1,compute2:2181";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop202:8123/default";
}

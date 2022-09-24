package com.daxian.cdc;


import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author: daxian
 * @Date: 2021/11/29 3:24 下午
 */
public class Ds {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ///TODO 2.开启检查点   Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,
//        // 需要从Checkpoint或者Savepoint启动程序
//        //2.1 开启Checkpoint,每隔5秒钟做一次CK  ,并指定CK的一致性语义
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        //2.2 设置超时时间为1分钟
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        //2.3 指定从CK自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2000L));
//        //2.4 设置任务关闭的时候保留最后一次CK数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/flinkCDC"));
//        //2.6 设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "daxian");



    }
}

//package com.daxian;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.DataTypes;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.table.descriptors.Json;
//import org.apache.flink.table.descriptors.Kafka;
//import org.apache.flink.table.descriptors.Schema;
//
///**
// * 1创建表的执行环境
// * 2、注册catalog
// * 3、在内部注册表
// * 4、执行书ql查询
// * 5、注册自定义函数
// * 6、将datastream转为表
// * 7、执行
// * <p>
// * example：
// * // 创建表的执行环境 val tableEnv = ...
// * // 创建一张表，用于读取数据 tableEnv.connect(...).createTemporaryTable("inputTable")
// * // 注册一张表，用于把计算结果输出 tableEnv.connect(...).createTemporaryTable("outputTable")
// * // 通过 Table API 查询算子，得到一张结果表 val result = tableEnv.from("inputTable").select(...)
// * // 通过 SQL查询语句，得到一张结果表 val sqlResult = tableEnv.sqlQuery("SELECT ... FROM inputTable ...")
// * // 将结果表写入输出表中 result.insertInto("outputTable")
// */
//public class KafkaToMysql {
//    public static void main(String[] args) throws Exception {
//
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
//        tenv.connect(new Kafka()
//                .version("universal")
//                .topic("dx")
//                .property("zookeeper.connect", "localhost:2181")
//                .property("bootstrap.servers", "localhost:9092")
//                .startFromLatest()
//        )
//                .inAppendMode()
//                .withFormat(new Json()
//                        .failOnMissingField(false)
//                        )
//                .withSchema(new Schema()
//                        .field("id", DataTypes.STRING())
//                        .field("name", DataTypes.STRING())
//                ).createTemporaryTable("demo");
//        String ml = "CREATE TABLE tt (\n" +
//                "   `id` VARCHAR,\n" +
//                "   `name` VARCHAR \n" +
//                ") WITH (\n" +
//                "   'connector.type' = 'jdbc', \n" +
//                "   'connector.url' = 'jdbc:mysql://118.24.109.221:3306/test', \n" +
//                "   'connector.table' = 'daxian', \n" +
//                "   'connector.username' = 'root', \n" +
//                "   'connector.password' = '1QAZ2wsx!@#',\n" +
//                "   'connector.write.flush.interval' = '2s',\n" +
//                "   'connector.write.flush.max-rows' = '5000',\n" +
//                "   'connector.lookup.max-retries' = '3')";
//        tenv.sqlUpdate(ml);
//        // String sql = "select * from demo";
//        // Table table = tenv.sqlQuery(sql);
//        Table table = tenv.from("demo");
//        table.insertInto("tt");
//        tenv.execute("demo test");
//    }
//}

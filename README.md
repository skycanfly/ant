# ant
A summary of what you have learned


### build方式
第一：先打包module模块，必须执行install命令，打包后，其他模块依赖module，上传到服务器集群跑的时候，
1/注意版本，将module模块放在flink的lib目录下， flink run的时候会去lib目录下查找文件。

mvn clean install   flink-module

其他模块打包：
mvn clean package -DskipTests


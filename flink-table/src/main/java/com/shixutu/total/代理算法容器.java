package com.shixutu.total;

/**
 * @Author: daxian
 * @Date: 2021/10/28 8:41 下午
 */
public class 代理算法容器 {
    private api a;
    private Mysql mysql;
    private Kafka kafka;
    private view可视化 v;

    public void 加载算法模型() {

    }
    public void  存储模型() {

    }

    public void 数据etl转换() {

    }

    public void 获取数据() {
        mysql.拉取数据();
        kafka.拉取数据();
        数据etl转换();
    }
}

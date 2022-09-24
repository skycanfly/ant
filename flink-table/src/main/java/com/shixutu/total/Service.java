package com.shixutu.total;

/**
 * @Author: daxian
 * @Date: 2021/10/28 9:51 下午
 */
public class Service {
    private  Mysql mysql;
    private  Kafka kafka;
    private  Drools drools;
    private 代理算法容器 d;
    private view可视化 v;


    public  void init(){
        drools.使用规则();
        drools.创建规则();
        d.获取数据();
        mysql.存储模型();
        v.分析偏差();
    }

    public  void 分析偏差(){

    }
    public void 获取数据(){


    }
    public  void delete(){}
    public   void api(){
    }
}

package com.shixutu.total;

/**
 * @Author: daxian
 * @Date: 2021/10/28 9:11 下午
 */
public class Drools {
    private Mysql mysql;
    private 代理算法容器 s;
    private api a;
    private Service service;


   public  void create(){
       创建规则();
       使用规则();
   }


    public  void 创建规则(){
        mysql.获取算法();

    }
    public  void 使用规则(){
       mysql.获取模型();

    }

}

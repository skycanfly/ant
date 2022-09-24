package com.shixutu.task;

/**
 * @Author: daxian
 * @Date: 2021/10/31 6:33 下午
 */
public class proxyService {
    private  mysql mysql;
    public  void  status(){}
    public  void start(){
        mysql.update();
    }
}

package com.shixutu.task;

/**
 * @Author: daxian
 * @Date: 2021/10/31 6:33 下午
 */
public class taskService {
    private proxyService proxyService;
    private  mysql mysql;
    public  void  start(){
        mysql.status();
        proxyService.start();
    }
}

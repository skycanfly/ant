package com.shixutu.chuli;

/**
 * @Author: daxian
 * @Date: 2021/11/2 7:55 下午
 */
public class BatchServer {
    private  ThirdServer thirdServer;
    private Excuteproxy excuteproxy;
    public  void queue(){
        crud();
    }
    public  void crud(){
        thirdServer.configInfo();
        excuteproxy.crud();
        excuteproxy.log();
    }
    public  void  callback(){

    }
}

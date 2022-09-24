package com.shixutu.chuli;

/**
 * @Author: daxian
 * @Date: 2021/11/2 7:56 下午
 */
public class Excuteproxy {
    private Yarn yarn;
    private BatchServer batchServer;
    public  void crud(){
        status();
        batchServer.callback();
        yarn.run();
    }
    public  void status(){ }
    public  void log(){ }
}

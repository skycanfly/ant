package com.shixutu.chuli;

/**
 * @Author: daxian
 * @Date: 2021/11/2 7:54 下午
 */
public class BathcServer {
    private BatchServer batchServer;
    private ThirdServer thirdServer;
    public  void crud(){
        thirdServer.configInfo();
        batchServer.queue();
    }

}

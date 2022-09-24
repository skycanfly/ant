package com.shixutu.ai;

/**
 * @Author: daxian
 * @Date: 2021/10/31 5:31 下午
 */
public class aiService {
    private  fileService fileService;
    private  mysql mysql;
    private  proxyService proxyService;
    public  void load(){
        mysql.加载算法();
        fileService.加载数据();
        proxyService.提交代理();
    }


    public  void Callback(){
        mysql.save();
    }
    public  void algorithmTraining(){ }
    public  void featureEngineering(){ }
    public  void ModelAssess(){ }
    public  void application(){ }
}

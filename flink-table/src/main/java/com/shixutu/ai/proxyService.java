package com.shixutu.ai;

/**
 * @Author: daxian
 * @Date: 2021/10/31 6:12 下午
 */
public class proxyService {
    private mysql mysql;
    private fileService fileService;
    private aiService aiService;

    public void 提交代理() {
        algorithmTraining();
        featureEngineering();
        ModelAssess();
        application();
        mysql.save();
        fileService.save();
        aiService.Callback();
    }


    public void algorithmTraining() {
    }

    public void featureEngineering() {
    }

    public void ModelAssess() {
    }

    public void application() {
    }
}

package com.daxian.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Author: Felix
 * Date: 2021/8/9
 * Desc:  线程池工具类
 */
public class ThreadPoolUtil {
    public static ThreadPoolExecutor pool ;

    /*
    int corePoolSize,       初始线程数量
    int maximumPoolSize,    最大线程数
    long keepAliveTime,     当线程池中空闲线程的数据超过corePoolSize，会在keepAliveTime时间后会销毁
    TimeUnit unit,          时间单位
    BlockingQueue<Runnable> workQueue   要执行的任务队列
    */
    public static ThreadPoolExecutor getInstance(){
        if(pool == null){
            synchronized(ThreadPoolExecutor.class){
                if(pool == null){
                    System.out.println("---开辟线程池---");
                    pool = new ThreadPoolExecutor(
                        4,20,300,TimeUnit.SECONDS,
                        new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }

        return pool;
    }
}

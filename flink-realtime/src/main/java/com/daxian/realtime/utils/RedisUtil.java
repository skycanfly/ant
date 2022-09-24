package com.daxian.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Author: Felix
 * Date: 2021/8/9
 * Desc: 获取操作Redis的Java客户端 Jedis
 */
public class RedisUtil {
    //声明JedisPool连接池
    private static JedisPool jedisPool;
    public static Jedis getJedis(){
        if(jedisPool == null){
            initJedisPool();
        }
        System.out.println("----获取Redis连接----");
        return jedisPool.getResource();
    }

    //初始化连接池对象
    private static void initJedisPool() {
        //连接池配置对象
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        //最大连接数
        jedisPoolConfig.setMaxTotal(100);
        //每次在连接的时候是否进行ping pong测试
        jedisPoolConfig.setTestOnBorrow(true);
        //连接耗尽是否等待
        jedisPoolConfig.setBlockWhenExhausted(true);
        //等待时间
        jedisPoolConfig.setMaxWaitMillis(2000);
        //最小空闲连接数
        jedisPoolConfig.setMinIdle(5);
        //最大空闲连接数
        jedisPoolConfig.setMaxIdle(5);
        jedisPool = new JedisPool(jedisPoolConfig,"hadoop202",6379,10000);
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
    }
}

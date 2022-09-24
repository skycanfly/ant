package com.daxian.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * Author: Felix
 * Date: 2021/8/10
 * Desc:
 */
public interface DimJoinFunction<T> {

    void join(T obj, JSONObject dimJsonObj) throws Exception;

    String getKey(T obj);
}

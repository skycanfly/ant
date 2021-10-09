package com.daxian.api.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: daxian
 * @Date: 2021/10/2 11:13 下午
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Event {
    private String name;
    private String url;
    private  Long time;



}

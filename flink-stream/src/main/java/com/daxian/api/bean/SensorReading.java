package com.daxian.api.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: daxian
 * @Date: 2021/10/1 9:39 上午
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class SensorReading {
    private String id;
    private Long time;
    private Double temperature;

}

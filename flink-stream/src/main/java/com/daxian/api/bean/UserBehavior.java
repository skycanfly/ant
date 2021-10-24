package com.daxian.api.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: daxian
 * @Date: 2021/10/17 11:52 下午
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class UserBehavior {

    public String userId;
    public String itemId;
    public String categoryId;
    public String behavior;
    public Long timestamp;
}

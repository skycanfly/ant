package com.daxian.realtime.utils;

import com.alibaba.fastjson.JSONObject;

import com.daxian.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: Felix
 * Date: 2021/8/7
 * Desc:  从Phoenix表中查询数据
 */
public class PhoenixUtil {

    private static Connection conn;

    private static void initConn() {
        try {
            //注册驱动
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            //获取连接
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            //设置操作的表空间
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //执行查询SQL，将查询结果集封装T类型对象，放到List
    public static <T> List<T> queryList(String sql, Class<T> clz) {
        if (conn == null) {
            initConn();
        }
        List<T> resList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //创建数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            rs = ps.executeQuery();
            //获取查询结果集的元数据信息
            ResultSetMetaData md = rs.getMetaData();
            //处理结果集
            // ID  | TM_NAME
            // 12  | aaa
            while (rs.next()) {
                //通过反射创建对应封装类型的对象
                T obj = clz.newInstance();
                //根据元数据获取表中的列名
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    String columnName = md.getColumnName(i);
                    Object cloumnValue = rs.getObject(i);
                    //通过BeanUtils工具类，给对象的属性赋值
                    BeanUtils.setProperty(obj, columnName,cloumnValue);
                }
                resList.add(obj);
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resList;
    }


    public static void main(String[] args) {
        List<JSONObject> jsonObjectList = queryList("select * from dim_base_trademark", JSONObject.class);
        System.out.println(jsonObjectList);
    }
}

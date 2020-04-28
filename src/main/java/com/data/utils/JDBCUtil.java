package com.data.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class JDBCUtil {

    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL =
            "jdbc:mysql://192.168.5.135:3316/databank5?useUnicode=true&amp;characterEncoding=utf8&amp;autoReconnect=true&amp;zeroDateTimeBehavior=convertToNull&amp;allowMultiQueries=true";
    private static final String MYSQL_USERNAME = "databank_135";
    private static final String MYSQL_PASSWORD = "yoYi_2016";

    /**
     * 获取mysql的连接对象
     * @return
     */
    public static Connection getConnection(){
        Connection conn = null;
        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            conn = DriverManager.getConnection(MYSQL_URL,MYSQL_USERNAME,MYSQL_PASSWORD);
        }catch (Exception e){
            e.printStackTrace();
        }
        return conn;
    }
}

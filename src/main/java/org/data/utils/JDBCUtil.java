package org.data.utils;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * 定义一个mysql的连接配置
 */
public class JDBCUtil {

    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
//        private static final String MYSQL_URL =
//            "jdbc:mysql://192.168.5.135:3316/databank5?useUnicode=true&amp;characterEncoding=utf8&amp;autoReconnect=true&amp;zeroDateTimeBehavior=convertToNull&amp;allowMultiQueries=true";
//    private static final String MYSQL_USERNAME = "databank_135";
//    private static final String MYSQL_PASSWORD = "yoYi_2016";
    private static final String MYSQL_URL =
            "jdbc:mysql://localhost:3306/movie?useUnicode=true&characterEncoding=utf-8";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "root";

    /**
     * 获取mysql的连接对象
     *
     * @return
     */
    public static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }
}


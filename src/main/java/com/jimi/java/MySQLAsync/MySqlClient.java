package com.jimi.java.MySQLAsync;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Author Administrator
 * @create 2019/8/26 15:39
 */


public class MySqlClient {

    private static String jdbcUrl = "jdbc:mysql://120.77.251.74/test?useSSL=false&allowPublicKeyRetrieval=true";
    private static String username = "root";
    private static String password = "jimi@123";
    private static String driverName = "com.mysql.jdbc.Driver";
    private static java.sql.Connection conn;
    private static PreparedStatement ps;

    static{
        try{
            Class.forName(driverName);
            conn = DriverManager.getConnection(jdbcUrl,username,password);
            ps = conn.prepareStatement("select user_id from user_relation u where u.imei = ?");
        } catch(ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public UserRelation query1(UserRelation ur){
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Long userId = 0l;
        try{
            ps.setString(1,ur.getImei());
            ResultSet rs = ps.executeQuery();
            if(!rs.isClosed() && rs.next()){
                userId = rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        ur.setUserId(userId);
        return ur;
    }

    public static void main(String[] args) {

        MySqlClient mySqlClient = new MySqlClient();

        UserRelation ur = new UserRelation();
        ur.setImei("000010137449945");
        long start = System.currentTimeMillis();
        System.out.println(start);
        mySqlClient.query1(ur);
        System.out.println(System.currentTimeMillis());
        System.out.println("end : " + (System.currentTimeMillis() - start));
        System.out.println(ur.toString());
    }
}
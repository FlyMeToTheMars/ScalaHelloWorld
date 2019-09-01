package com.jimi.java.MySQLSource;

import java.sql.*;

/**
 * @Author Administrator
 * @create 2019/8/22 10:54
 */
public class JdbcTest {
    public static void main(String[] args) throws SQLException {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://120.77.251.74/test";
        String username = "root";
        String password = "jimi@123";
        Connection connection = null;
        Statement statement = null;

        PreparedStatement preparedStatement = null;

        String sql = "select stuid,stuname,stuaddr,stusex from student";

        try{
            // 加载驱动
            Class.forName(driver);

            // 创建连接
            connection = DriverManager.getConnection(url, username, password);
            /**
             *
             * 有关Statement的总结：
             *  Statement、PreparedStatement和CallableStatement都是接口(interface)。
             *  Statement继承自Wrapper、PreparedStatement继承自Statement、CallableStatement继承自PreparedStatement。
             *
             * 区别：
             *  Statement接口提供了执行语句和获取结果的基本方法；
             *  PreparedStatement接口添加了处理 IN 参数的方法；
             *  CallableStatement接口添加了处理 OUT 参数的方法。
             *
             * 特性：
             *  a.Statement:
             *  普通的不带参的查询SQL；支持批量更新,批量删除;
             *  b.PreparedStatement:
             *  可变参数的SQL,编译一次,执行多次,效率高;
             *  安全性好，有效防止Sql注入等问题;
             *  支持批量更新,批量删除;
             *  c.CallableStatement:
             *  继承自PreparedStatement,支持带参数的SQL操作;
             *  支持调用存储过程,提供了对输出和输入/输出参数(INOUT)的支持;
             *
             * Statement每次执行sql语句，数据库都要执行sql语句的编译 ，
             * 最好用于仅执行一次查询并返回结果的情形，效率高于PreparedStatement。
             *
             * PreparedStatement是预编译的，使用PreparedStatement有几个好处
             * 1. 在执行可变参数的一条SQL时，PreparedStatement比Statement的效率高，因为DBMS预编译一条SQL当然会比多次编译一条SQL的效率要高。
             * 2. 安全性好，有效防止Sql注入等问题。
             * 3.  对于多次重复执行的语句，使用PreparedStament效率会更高一点，并且在这种情况下也比较适合使用batch；
             * 4.  代码的可读性和可维护性。
             *
             *
             *
             *
             * */

            // 获取执行语句 执行成功后 又尝试了preparedStatement 也可以
//            statement = connection.createStatement();


            preparedStatement = connection.prepareStatement(sql);

            // 执行查询，获得结果集
            ResultSet resultSet  = preparedStatement.executeQuery(sql);

            while (resultSet.next()){
                Student student = new Student(
                        resultSet.getInt("stuid"),
                        // trim():返回一个新的字符串。这个字符串将删除了原始字符串头部和尾部的空格
                        resultSet.getString("stuname").trim(),
                        resultSet.getString("stuaddr").trim(),
                        resultSet.getString("stusex").trim()
                );
                System.out.println(student);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.close();
            }
            if (statement != null) {
                connection.close();
            }
        }
    }
}

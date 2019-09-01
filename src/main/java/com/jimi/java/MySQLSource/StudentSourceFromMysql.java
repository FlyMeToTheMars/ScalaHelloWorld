package com.jimi.java.MySQLSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author Administrator
 * @create 2019/8/22 11:51
 */

public class StudentSourceFromMysql extends RichSourceFunction<Student> {

    private PreparedStatement ps = null;
    private Connection connection = null;
    String driver = "com.mysql.jdbc.Driver";
    String url = "jdbc:mysql://120.77.251.74/test";
    String username = "root";
    String password = "jimi@123";

    // open()方法中建立拦截，不用每次invoke的时候都需要建立连接和释放连接
    // org.apach.flink.common.functions.AbstractRichFunction#open
    @Override
    public void open(Configuration parameters) throws Exception{
        super.open(parameters);
        connection = getConnection();
        String sql = "select stuid,stuname,stuaddr,stusex from student;";
        // 获取 执行语句
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Student student = new Student(
                    resultSet.getInt("stuid"),
                    resultSet.getString("stuname").trim(),
                    resultSet.getString("stuaddr").trim(),
                    resultSet.getString("stusex").trim()
            );
            ctx.collect(student);   // 发送结果
        }
    }

    /**
     *
     * 取消一个job时出发的方法
     * org.apach.flink.streaming.api.functions.source.SourceFunction#cancel()
     *
     * */
    @Override
    public void cancel() {

    }

    //关闭数据库连接
    public void close() throws Exception{
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    public Connection getConnection(){
        try {
            //加载驱动
            Class.forName(driver);

            // 创建连接
            connection = DriverManager.getConnection(url,username,password);

        } catch (Exception e) {
            System.out.println("____________________MySQL Get Connection exception, Msg = " + e.getMessage());
            e.printStackTrace();
        }
        return connection;
    }
}

package com.jimi.scala.MySQLAysncIO

import java.sql.{Connection, DriverManager, ResultSet}

/**
  * @Author Administrator
  * @create 2019/8/22 18:29
  */
object ScalaJDBCTest {

  // 如果用case class是不是更好一点呢
  def main(args: Array[String]): Unit = {

    val conn_str = "jdbc:mysql://120.77.251.74/test?user=root&password=jimi@123"

    classOf[com.mysql.jdbc.Driver]

    // Setup the connection
    val conn = DriverManager.getConnection(conn_str)

    try{
      // 修改 Statement 为 PreparedStatement 还有几个参数都试一下，找一下不能read的原因。

      // Configure to be Read Only

      val statement = conn.createStatement(ResultSet.CONCUR_READ_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
      val rs: ResultSet = statement.executeQuery("select stuid,stuname,stuaddr,stusex from student")

      while (rs.next()) {
        print(rs.getInt("stuid") + " ")
        print(rs.getString("stuname")+ " ")
        print(rs.getString("stuaddr")+ " ")
        println(rs.getString("stusex"))
      }
    } catch {
      case e:Exception => e.printStackTrace
    } finally {
      conn.close
    }
  }
}
package com.jimi.scala.MySQLAysncIO

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.JSON
import com.mysql.jdbc.Driver
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.concurrent.{ExecutionContext, Future}

/**
  * @Author Administrator
  * @create 2019/8/26 14:30
  */
object PrepareStatement {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val properties = new Properties()

    //kafka位置 老版本的 kafka是配置zookeeper地址
    properties.setProperty("bootstrap.servers","master126:9092")
    properties.setProperty("zookeeper.connect","master126:2181")

    val topic = "flink-topic"

    properties.setProperty("group.id", "cloudera_mirrormaker")

    val kafkaStream: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer(topic,new SimpleStringSchema(),properties)

    kafkaStream.setStartFromLatest()

    import org.apache.flink.api.scala._
    val stream: DataStream[String] = env.addSource(kafkaStream)

    val resultStream: DataStream[String] =AsyncDataStream.orderedWait(stream,new AsyncDatabaseRequest, 3000, TimeUnit.MILLISECONDS, 100)

    resultStream.print

    env.execute("Mysql & kafka")
  }

  class AsyncDatabaseRequest extends AsyncFunction[String,String]{

    override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {

      val conn_str = "jdbc:mysql://120.77.251.74/test?user=root&password=jimi@123"

      Future{

        // 获取Kafka日志的imei
        val imei = JSON.parseObject(input).get("imei").toString

        // 懒加载执行获得conn
        lazy val CD: Class[Driver] = classOf[com.mysql.jdbc.Driver]
        lazy val conn: Connection = DriverManager.getConnection(conn_str)

//        val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

//        val user_id: ResultSet = statement.executeQuery(s"select user_id from user_relation u where u.imei = $imei")
        val ps: PreparedStatement = conn.prepareStatement(s"select user_id from user_relation u where u.imei = $imei")

        val rs: ResultSet = ps.executeQuery()

        resultFuture.complete(Seq(rs.getString("user_id")))

        conn.close()

      }(ExecutionContext.global)
    }
  }
}

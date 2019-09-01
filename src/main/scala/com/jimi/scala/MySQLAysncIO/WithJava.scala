package com.jimi.scala.MySQLAysncIO

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import com.alibaba.fastjson.{JSON, JSONObject}
import com.jimi.java.MySQLAsync.{AsyncFunctionForMysqlJava, UserRelation}
import com.jimi.scala.MySQLAysncIO.PrepareStatement.AsyncDatabaseRequest
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}

/**
  * @Author Administrator
  * @create 2019/8/27 10:44
  */
object WithJava {
  case class ReportAlarm(
                          imei: String,
                          addr:String,
                          alertType:String,
                          gpsTime: String,
                          fenceId:String,
                          lat:String,
                          lng: String,
                          postTime: String,
                          offlineTime: String,
                          `type`: String,
                          iccid:String,
                          imsi: String,
                          accStatus: String,
                          time: String)

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

    // Kafka流 String类型
  }
}
package com.jimi.scala.KafkaDemo

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * @Author Administrator
  * @create 2019/8/23 15:11
  */

object kafkaConsumer {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","master126:9092")
//    properties.setProperty("zookeeper.connect","master126:2181")
    val topic = "flink-topic"
    properties.setProperty("group.id", "cloudera_mirrormaker")

    val kafkStream: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer(topic,new SimpleStringSchema(),properties)
    kafkStream.setStartFromLatest()

    import org.apache.flink.api.scala._
    val stream: DataStream[String] = env.addSource(kafkStream)

    stream.print()

    env.execute("kafkaStreamPrintln")

  }
}

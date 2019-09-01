package com.jimi.scala.redis

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import scala.concurrent.{ExecutionContext, Future}

object demo9 {
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

    val stream: DataStream[String] = env.addSource(kafkaStream)
    val resultStream: DataStream[(String, String)] =AsyncDataStream.orderedWait(stream,new RedisAsyncFunctionTuple(), 10000, TimeUnit.MILLISECONDS, 10000)
    val countSteam: DataStream[(String, Int)] = resultStream
//      .filter()
      .map(line => (line.toString(),1))
        .keyBy(0)
        .timeWindow(Time.seconds(100000))
//        .aggregate(new CountAgg(), new WindowResultFunction())
        .sum(1)

    resultStream.print
    env.execute("redis & kafka")
  }
}

class RedisAsyncFunctionTuple extends  AsyncFunction[String,Tuple2[String,String]]{

  lazy val pool: JedisPool = new JedisPool(new JedisPoolConfig,"172.16.10.104",6379,100,"jimi@123")

  override def asyncInvoke(input: String, resultFuture: ResultFuture[Tuple2[String,String]]): Unit = {

    val imei1: String = JSON.parseObject(input).get("imei").toString
    Future {
      //获取kafka日志的imei号
      val imei = JSON.parseObject(input).get("imei").toString

      //从redis中获取imei对应的userid
      //      println(pool.getNumActive)

      val jedis = pool.getResource
      val useridJson: String =jedis.hget("DC_IMEI_APPID",imei)
      val userid: String = JSON.parseObject(useridJson).get("userId").toString

      resultFuture.complete(Seq(Tuple2(userid,imei)))


      /*

      Scala的 Seq将是Java的List，Scala 的 List将是Java的 LinkedList。

      请注意，Seq是一个trait，它相当于Java的接口，但相当于即将到来的防御者方法。 Scala的List是一个抽象类，由Nil和::扩展，这是List的具体实现。
      所以，在Java的List是一个接口，Scala的List是一个实现。
      除此之外，Scala的List是不可变的，这不是LinkedList的情况。事实上，Java没有等价的不可变集合(只读的东西只保证新的对象不能改变，但你仍然可以改变旧的，因此，“只读”一个)。
      Scala的List是由编译器和库高度优化的，它是函数式编程中的基本数据类型。然而，它有限制，它不足以并行编程。这些天，Vector是一个比List更好的选择，但习惯是很难打破。
      Seq是一个很好的泛化序列，所以如果你编程到接口，你应该使用它。注意，实际上有三个：collection.Seq，collection.mutable.Seq和collection.immutable.Seq，它是后一个是“默认”导入到范围。
      还有GenSeq和ParSeq。后面的方法在可能的情况下并行运行，前者是Seq和ParSeq的父代，这是当代码的并行性无关紧要的合适的泛化。它们都是相对新引入的，因此人们不会使用它们。

      */

      pool.returnResource(jedis)
    }(ExecutionContext.global)
  }
}

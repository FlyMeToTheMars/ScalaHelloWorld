/*
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import com.alibaba.fastjson.JSON
import redis.clients.jedis.Jedis
import java.util.concurrent.TimeUnit
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import scala.concurrent.{ExecutionContext, Future}

object AsynFlinkRedis {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    //kafka位置 老版本的 kafka是配置zookeeper地址
    properties.setProperty("bootstrap.servers","master126:9092")
    properties.setProperty("zookeeper.connect","master126:2181")
    val topic = "flink-topic"
    properties.setProperty("group.id", "cloudera_mirrormaker")

    val kafkStream = new FlinkKafkaConsumer(topic,new SimpleStringSchema(),properties)

    val stream = env.addSource(kafkStream)

    val resultStream: DataStream[String]  =AsyncDataStream.unorderedWait(stream,new RedisAsyncFunction(), 1000, TimeUnit.MILLISECONDS, 100)

    resultStream.print()
    env.execute()
  }
}

// 通过继承 AsyncFunction 实现异步查询redis
class RedisAsyncFunction extends AsyncFunction[String,String]{

  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {

    Future {

      //获取kafka日志的imei号
      val imei = JSON.parseObject(input).get("imei").toString

      //从redis中获取imei对应的userid

      val jedis: Jedis = new Jedis("172.16.10.104",6379,0)
      jedis.auth("jimi@123")

      val useridJson = jedis.hget("DC_IMEI_APPID",imei)

      resultFuture.complete(Seq(useridJson))

    }(ExecutionContext.global)

  }
}*/

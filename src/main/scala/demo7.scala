
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
import org.apache.flink.table.api.TableEnvironment


object demo7 {
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

  case class KafkaMsg(key: String, value: String, topic: String, partiton: Int, offset: Long)
  class TypedKeyedDeserializationSchema extends KeyedDeserializationSchema[KafkaMsg] {
    def deserialize(key: Array[Byte],
                    value: Array[Byte],
                    topic: String,
                    partition: Int,
                    offset: Long
                   ): KafkaMsg =
      KafkaMsg(
        new String(key),
        new String(value),
        topic,
        partition,
        offset
      )

    def isEndOfStream(e: KafkaMsg): Boolean = false

    def getProducedType(): TypeInformation[KafkaMsg] = createTypeInformation

  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // create a TableEnvironment for streaming queries

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","master126:9092")
    properties.setProperty("zookeeper.connect","master126:2181")
    val topic = "flink-topic"
    properties.setProperty("group.id","cloudera_mirrormaker")


    val textConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer(topic,new SimpleStringSchema(),properties)

    val KafkaSource: DataStream[JSONObject] = env.addSource(textConsumer).map(line => JSON.parseObject(line))

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    env.execute("try")

  }
}

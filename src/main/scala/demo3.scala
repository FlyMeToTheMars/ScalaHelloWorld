import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
import org.apache.flink.util.Collector

object demo3 {
  case class KafkaMsg(key: String, value: String, topic: String, partiton: Int, offset: Long)

  class TypedKeyedDeserializationSchema extends KeyedDeserializationSchema[KafkaMsg] {
    def deserialize(key: Array[Byte],
                    value: Array[Byte],
                    topic: String,
                    partition: Int,
                    offset: Long
                   ): KafkaMsg =
      KafkaMsg(new String(key),
        new String(value),
        topic,
        partition,
        offset
      )

    def isEndOfStream(e: KafkaMsg): Boolean = false

    def getProducedType(): TypeInformation[KafkaMsg] = createTypeInformation
  }

  class MyTimeTimestampsAndWatermarks extends AssignerWithPunctuatedWatermarks[(String,String)] with Serializable{
    //生成时间戳
    override def extractTimestamp(element: (String,String), previousElementTimestamp: Long): Long = {
      System.currentTimeMillis()
    }

    override def checkAndGetNextWatermark(lastElement: (String, String), extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp -1000)
    }
  }

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val properties = new Properties()
    //kafka位置 老版本的 kafka是配置zookeeper地址
    properties.setProperty("bootstrap.servers","mater126:9092")
    properties.setProperty("zookeeper.connect","master126:2181")
    val topic = "flink-topic"
    properties.setProperty("group.id", "cloudera_mirrormaker")

    val consumer = new FlinkKafkaConsumer(topic,new TypedKeyedDeserializationSchema(),properties)
    val text: DataStream[Tuple2[String, String]]  = env.addSource(consumer).map(line => Tuple2(line.value,"1")).assignTimestampsAndWatermarks(new MyTimeTimestampsAndWatermarks())

    text.keyBy(0)
      .process(new CountWithTimeoutFunction()).setParallelism(1).print()
    env.execute()
    case class CountWithTimestamp(key: String, count: Long, lastModified: Long)

    class CountWithTimeoutFunction extends KeyedProcessFunction[Tuple, (String, String), (String, Long)]{
      lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext
        .getState(new ValueStateDescriptor[CountWithTimestamp]("myState1", classOf[CountWithTimestamp]))

      override def processElement(value: (String, String),
                                  ctx: KeyedProcessFunction[Tuple, (String, String), (String, Long)]#Context,
                                  out: Collector[(String, Long)]) = {
        println(new SimpleDateFormat("mm").format(new Date((ctx.timestamp()).toLong)))
        //你可以定时删除sate，就是控制hour ='00',minute = '01'， 这样误差就是丢失1分钟的数据
        if(new SimpleDateFormat("mm").format(new Date((ctx.timestamp()).toLong)).toString == "59"){
          println("开始清理")
          state.clear()
        }

        // initialize or retrieve/update the state
        val current: CountWithTimestamp = state.value match {
          case null =>
            CountWithTimestamp(value._1, 1, ctx.timestamp())
          case CountWithTimestamp(key, count, lastModified) =>
            CountWithTimestamp(key, count + 1, ctx.timestamp())
        }

        // write the state back
        state.update(current)
        println(ctx.timestamp()+"==")
        println(current.lastModified+"++")

        // schedule the next timer 6 seconds from the current event time
        ctx.timerService.registerProcessingTimeTimer(current.lastModified + 6000)
      }

      override def onTimer(
                            timestamp: Long,
                            ctx: KeyedProcessFunction[Tuple, (String, String), (String, Long)]#OnTimerContext,
                            out: Collector[(String, Long)]): Unit = {
        println(timestamp+"====")
        state.value match {

          case CountWithTimestamp(key, count, lastModified) if (timestamp == lastModified + 6000) =>
            out.collect((key, count))
          case _ => println(CountWithTimestamp)
        }
      }
    }
  }
}

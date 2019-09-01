package com.jimi.scala.WaterMarkDemo

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Author Administrator
  * @create 2019/8/30 9:19
  */

object WatermarkTest {
  /**
    * 1.接收socket数据
    * 2.将每行数据按照字符串分割，每行map成一个tuple类型(code,time)
    * 3.抽取timestmap生成watermark
    * 4.并打印(code,time,格式化的time，currentMaxTimestamp，currentMaxTimestamp的格式化时间，watermark时间)
    * 5.3event time 每隔3s出发一次窗口，输出(code，窗口内元素个数，窗口内最早元素时间，窗口内最晚元素时间，窗口自身开始时间，窗口自身结束时间)
    *
    *
    * */

  /*
  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      System.err.println("USAGE:\nSocketWatermarkTest <hostname> <port>")
      return
    }

    val hostName = args(0)
    val port = args(1).toInt

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val input = env.socketTextStream(hostName,port)

    import org.apache.flink.api.scala._

    val inputMap: DataStream[(String, Long)] = input.map(f => {
      val arr = f.split("\\W+")
      val code = arr(0)
      val time = arr(1).toLong
      (code,time)
    })

    val watermark = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
      /**
        * AssignerWithPeriodicWatermarks中有抽取timestamp和生成watermark两个方法，在执行的时候，它是先抽取timestamo
        * 后生成watermark，因此我们在这里print的watermark的时间，其实是上面一条的watermark的时间。
        * */
      var currentMaxTimestamp = 0L

      // 最大允许的乱序事件是10s
      val maxOutOfOrderness = 10000L

      var a: Watermark = null

      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = {
        a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        a
      }

      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._2
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        println("timestamp:" + element._1 + "," + element._2 + "|" + format.format(element._2) + "," + currentMaxTimestamp + "|" + format.format(currentMaxTimestamp) + "," + a.toString)
        timestamp
      }
    })

    val window = watermark
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new WindowFunctionTest)

    window.print()

    env.execute()

  }

  class WindowFunctionTest extends WindowFunction[(String,Long),(String, Int,String,String,String,String),String,TimeWindow]{

    override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int,String,String,String,String)]): Unit = {
      val list = input.toList.sortBy(_._2)
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      out.collect(key,input.size,format.format(list.head._2),format.format(list.last._2),format.format(window.getStart),format.format(window.getEnd))
    }
  }
  */


  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("USAGE:\nSocketWatermarkTest <hostname> <port>")
      return
    }

    val hostName = args(0)
    val port = args(1).toInt

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val input = env.socketTextStream(hostName,port)

    val inputMap = input.map(f=> {
      val arr = f.split("\\W+")
      val code = arr(0)
      val time = arr(1).toLong
      (code,time)
    })

    val watermark = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String,Long)] {

      var currentMaxTimestamp = 0L
      val maxOutOfOrderness = 10000L
      //最大允许的乱序时间是10s

      var a : Watermark = null

      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")



      override def extractTimestamp(t: (String,Long), l: Long): Long = {
        val timestamp = t._2
        println("-----" + timestamp)
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        println("timestamp:" + t._1 +","+ t._2 + "|" +format.format(t._2) +","+  currentMaxTimestamp + "|"+ format.format(currentMaxTimestamp) + ","+ a.toString)
        timestamp
      }

      override def getCurrentWatermark: Watermark = {
        a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        println("currentMaxTimestamp = " + currentMaxTimestamp,"maxOutOfOrderness = "+maxOutOfOrderness,a)
        a
      }
    })

    val window: DataStream[(String, Int, String, String, String, String)] = watermark
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new WindowFunctionTest)

    window.print()

    env.execute()
  }

  class WindowFunctionTest extends WindowFunction[(String,Long),(String, Int,String,String,String,String),String,TimeWindow]{
    // windowFunctionTest 控制流入窗口进出的数据
    override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int,String,String,String,String)]): Unit = {
      val list = input.toList.sortBy(_._2)
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      out.collect(key,input.size,format.format(list.head._2),format.format(list.last._2),format.format(window.getStart),format.format(window.getEnd))
    }
  }
}
/**
  * nc -lk 9876 在Linux 开启命令行验证watermark
  *
  * 输入
  *
  * 000001,1461756862000
  *
  * 控制台输出：
  *
  * timestamp:000001,1461756862000|2016-04-27 19:34:22.000,1461756862000|2016-04-27 19:34:22.000,Watermark @ -10000
  * 对照输出代码：
  * println("timestamp:" + element._1 + "," + element._2 + "|" + format.format(element._2) + "," + currentMaxTimestamp + "|" + format.format(currentMaxTimestamp) + "," + a.toString)
  * 相当于: key + 时间戳 + | + 时间戳转译 + currentMaxTimestamp + 时间戳转译 + 时间戳.toString(Watermark @ 时间戳)
  * watermark:(currentMaxTimestamp - maxOutOfOrderness)
  * 这里的watermark的值是-10000，即0-10000得到的。
  * AssignerWithPeriodicWatermarks 子类是每隔一段时间执行的，这个具体由ExecutionConfig.setAutoWatermarkInterval设置，
  * 如果没有设置会一直调用getCurrentWatermark，之所以会出现-10000，是因为没有数据进入窗口，所以一直是-100000，但是getCurrentWatermark方法不是在执行extractTimestamp才执行。
  *
  * 所以： 每条记录打印出的watermark，都应该是上一条的watermark，为了方便观察，
  *
  *
  *
  *
  * */
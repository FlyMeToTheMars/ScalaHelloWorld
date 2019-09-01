package com.jimi.scala.UserBehaviorAnalysis


import java.sql.Timestamp
import java.util

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.FunctionParameter
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

/**
  * @Author Administrator
  * @create 2019/8/29 9:15
  */

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {

    // 创建env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设定Time类型为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 为了打印到控制台的结果不乱序，我们设置全局的并发为1，对结果并没有影响
    env.setParallelism(1)

    import org.apache.flink.api.scala._
    val stream = env
      .readTextFile("C:\\Projects\\my-flink-project-master\\src\\main\\resources\\UserBehavior.csv")
      .map(line => {
        val linearray = line.split(",")
        UserBehavior(linearray(0).toLong, linearray(1).toLong,linearray(2).toInt,linearray(3),linearray(4).toLong)
      })
      // 指定时间戳和watermark
      .assignAscendingTimestamps(_.timestamp*1000)
      // 过滤出点击事件
        .filter(_.behavior == "pv")
      // 设置滑动窗口，统计点击量
        .keyBy("itemId")
        .timeWindow(Time.minutes(60),Time.minutes(5))
      // aggregate 实现了AggregateFunction 的功能，功能是统计窗口中的条数，遇到一条就加一
      // WindowFunction将每个key每个窗口聚合后的结果带上其他信息输出，我们这里实现的WindowResultFunction将主键商品ID，窗口，点击量
      // 封装成了ItemViewCount进行输出。
        .aggregate(new CountAgg,new WindowResultFunction)
        .keyBy(1)
      /**
        * ProcessFunction 是Flink提供的一个Low-level API，用于实现更高级的功能。
        * 它主要提供了定时器timer的点击量数据，由于Watermark的进度是全局的，在processElement方法中，
        * 每当收到一条数据ItemViewCount，我们就注册了一个windowEnd+1 的定时器（Flink框架会自动忽略同一时间的重复注册）
        * wondowEnd+1的定时器被触发时，意味着收到了windowEnd+1的watermark
        * */
        .process(new TopNHotItems(3))
        .print()

    env.execute("HotJob")
  }
  /**
    * 这里我们需要统计业务时间上的每小时的点击量，所以要基于EventTime来处理。
    * 如果要基于EventTime来处理。那么如果让Flink按照我们想要的业务时间来处理呢？这里主要有两件事情要坐。
    * 第一件是告诉Flink我们现在按照EventTime进行处理。Flink默认使用ProcessingTime处理，所以我们要显式指定如下。
    * env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    * 第二件事情是指定如何获得业务时间，以及生成Watermark，Watermark是用来追踪业务事件的概念
    * 可以理解成EventTime世界中的时钟，用来指示当前褚力到什么时刻的数据了。由于我们的数据源的数据已经经过整理。
    * 也就是事件的时间戳是单调递增的，所以可以将每条数据的业务时间就当作watermark。
    * 这里我们用assignAscendingTimestamps来实现时间戳的抽取和Watermark的生成。
    * 但是真实的业务场景一般都是乱序的，所以一般不用assignAsscendingTimeStamps，而是使用
    * BoundedOutOfOrdernessTimestampExtractor
    *
    * 这样我们就得到了一个带有时间标记的数据流，后面能做一些窗口的操作。
    * */


  // COUNT统计的聚合函数实现，每出现一条记录就加一
  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L
    override def add(userBehavior: UserBehavior, acc: Long): Long = acc + 1
    override def getResult(acc: Long): Long = acc
    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
  }

  // 用于输出窗口的结果
  // WindowFunction[IN, OUT, KEY, W <: Window] 根据Key和window 指定自己的输出
  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, aggregateResult: Iterable[Long],
                       collector: Collector[ItemViewCount]) : Unit = {
      val itemId: Long = key.asInstanceOf[Tuple1[Long]]._1
      val count = aggregateResult.iterator.next
      collector.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }

  // 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
    private var itemState : ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 命名状态变量的名字和状态变量的类型
      val itemsStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])
      // 定义状态变量
      itemState = getRuntimeContext.getListState(itemsStateDesc)
    }

    override def processElement(input: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      // 每条数据都保存到状态中
      itemState.add(input)
      // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
      // 也就是当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
      context.timerService.registerEventTimeTimer(input.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 获取收到的所有商品点击量
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (item <- itemState.get) {
        allItems += item
      }
      // 提前清除状态中的数据，释放空间
      itemState.clear()
      // 按照点击量从大到小排序
      val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      // 将排名信息格式化成 String, 便于打印
      val result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

      for(i <- sortedItems.indices){
        val currentItem: ItemViewCount = sortedItems(i)
        // e.g.  No1：  商品ID=12224  浏览量=2413
        result.append("No").append(i+1).append(":")
          .append("  商品ID=").append(currentItem.itemId)
          .append("  浏览量=").append(currentItem.count).append("\n")
      }
      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }
}
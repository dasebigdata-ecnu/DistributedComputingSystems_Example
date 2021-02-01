package cn.edu.ecnu.flink.examples.scala.integersum

import java.text.SimpleDateFormat

import cn.edu.ecnu.flink.examples.scala.integersum.producer.Producer
import cn.edu.ecnu.flink.examples.scala.integersum.trigger.CustomerTriggerWithAccumulation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object DataflowModel_How {
  def run(args: Array[String]): Unit = {
    /* |步骤1： 创建StreamExecutionEnvironment对象| */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    /* |步骤2：按应用逻辑使用操作算子编写DAG，操作算子包括数据源、转换和数据池等| */
    val source = env.addSource(new Producer(false))
    val sink = source.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(120L)))
      // |自定义触发器：在水位线机制的基础上，在处理时间域上每隔一分钟输出一次结果。同时，迟到数据修正窗口结果|
      .trigger(new CustomerTriggerWithAccumulation(60L))
      // |设置允许延迟时间为300s|
      .allowedLateness(Time.seconds(300L))
      .apply(new myWindowFunction)
    sink.print()

    /* |步骤3：触发程序执行| */
    env.execute("Dataflow Model-How")
  }

  val sdf = new SimpleDateFormat("HH:mm:ss")

  class myWindowFunction extends WindowFunction[Tuple2[String, Integer], String, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Integer)], out: Collector[String]): Unit = {
      // |记录整数的累加和|
      var sum = 0
      // |获取窗口中键值对的迭代器|
      val it = input.iterator
      // |遍历窗口中的键值对，并对整数进行求和|
      while (it.hasNext) {
        val next = it.next()
        sum = sum + next._2
      }
      // |以字符串形式返回形如”the sum of window [12:00:00,12:02:00) is 14”的窗口函数结果|
      val res = "the sum of window [" + sdf.format(window.getStart) + "," + sdf.format(window.getEnd) + ") is " + sum
      out.collect(res)
    }
  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}

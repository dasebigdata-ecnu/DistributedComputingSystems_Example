package cn.edu.ecnu.flink.examples.scala.integersum.producer

import java.text.SimpleDateFormat
import java.util

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark

class Producer(var isBounded: Boolean) extends SourceFunction[Tuple2[String, Integer]] {
  final val data = util.Arrays.asList(
    new Tuple3[String, Integer, String]("dataflow", 5, "12:00:30"),
    new Tuple3[String, Integer, String]("dataflow", 7, "12:02:30"),
    new Tuple3[String, Integer, String]("dataflow", 3, "12:03:45"),
    // |水位线|
    new Tuple3[String, Integer, String]("dataflow", null, "12:02:00"),
    new Tuple3[String, Integer, String]("dataflow", 4, "12:03:50"),
    new Tuple3[String, Integer, String]("dataflow", 3, "12:04:30"),
    new Tuple3[String, Integer, String]("dataflow", 8, "12:03:30"),
    // |水位线|
    new Tuple3[String, Integer, String]("dataflow", null, "12:04:00"),
    // |水位线|
    new Tuple3[String, Integer, String]("dataflow", null, "12:06:00"),
    new Tuple3[String, Integer, String]("dataflow", 3, "12:06:30"),
    new Tuple3[String, Integer, String]("dataflow", 9, "12:01:30"),
    new Tuple3[String, Integer, String]("dataflow", 8, "12:07:30"),
    new Tuple3[String, Integer, String]("dataflow", 1, "12:07:50"),
    // |水位线|
    new Tuple3[String, Integer, String]("dataflow", null, "12:08:00"))

  // |每条记录的处理时间间隔|
  final val processInterval = util.Arrays.asList(40, 15, 25, 10, 5, 15, 30, 10, 10, 30, 20, 40, 20, 20)

  final val sdf = new SimpleDateFormat("HH:mm:ss")

  override def run(ctx: SourceFunction.SourceContext[(String, Integer)]): Unit = {
    waitForMinute()
    for (i <- 0 to 13) {
      // |记录发送延迟时间|
      Thread.sleep(processInterval.get(i) * 1000)
      val timestamp = sdf.parse(data.get(i)._3).getTime
      val value = data.get(i)._2
      // |若为水位线记录且输入数据作为无界数据集，则生成系统的水位线|
      if (value == null) {
        if (!isBounded) {
          ctx.emitWatermark(new Watermark(sdf.parse(data.get(i)._3).getTime))
        }
      }
      else {
        // |设置键值对的事件时间并发送至下游|
        ctx.collectWithTimestamp(new Tuple2[String, Integer](data.get(i)._1, value), timestamp)
      }
    }
  }

  override def cancel(): Unit = {}

  def waitForMinute(): Unit = {
    val interval = 60 * 1000
    val timestamp = System.currentTimeMillis()
    Thread.sleep(interval - (timestamp % interval))
  }
}
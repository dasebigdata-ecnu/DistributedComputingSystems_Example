package cn.edu.ecnu.flink.examples.scala.integersum.trigger

import java.text.SimpleDateFormat
import java.util

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class CustomerTriggerWithAccumulation extends Trigger[Any, TimeWindow] {
  val sdf = new SimpleDateFormat("HH:mm:ss")

  // |以自定义状态形式保存每个窗口中处理时间定时器所对应的触发时间|
  private lazy val processTimerStateDescriptor: ReducingStateDescriptor[Long] = new ReducingStateDescriptor[Long]("processTimer", new Update, classOf[Long])

  // |基于处理时间触发间隔|
  var interval = 60L

  // |构造函数|
  def this(interval: Long) {
    this()
    this.interval = interval * 1000
  }

  // |当有记录进入相应窗口时触发器将调用此方法|
  override def onElement(element: Any, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    // |从自定义状态中获取处理时间定时器所对应的触发时间|
    val fireTimestamp = ctx.getPartitionedState(processTimerStateDescriptor)
    // |窗口中进入第一条记录，处理时间定时器所对应的触发时间状态不存在|
    if (fireTimestamp.get == null) {
      val timestamp = ctx.getCurrentProcessingTime
      // |计算窗口的下一次触发时间|
      val start = timestamp - (timestamp % interval)
      val nextFireTimestamp = start + interval
      // |注册处理时间定时器|
      ctx.registerProcessingTimeTimer(nextFireTimestamp)
      // |将处理时间定时器所对应触发时间存入自定义状态|
      fireTimestamp.add(nextFireTimestamp)
    }
    // |迟到记录处理|
    if (window.maxTimestamp <= ctx.getCurrentWatermark) {
      System.out.println("迟到记录触发 ...")
      // |触发窗口操作时，调用窗口函数进行计算并保留窗口状态|
      TriggerResult.FIRE
    } else {
      // |根据记录所在窗口的最大时间戳，注册事件时间定时器|
      ctx.registerEventTimeTimer(window.maxTimestamp)
      // |对窗口不采取任何操作|
      TriggerResult.CONTINUE
    }
  }

  // |当注册的处理时间定时器到达指定时间时调用此方法|
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    val fireTimestamp = ctx.getPartitionedState(processTimerStateDescriptor)
    val timestamp = fireTimestamp.get
    // |更新自定义状态中的触发时间|
    fireTimestamp.add(timestamp + interval)
    // |根据窗口下一次触发的处理时间，注册处理时间定时器|
    ctx.registerProcessingTimeTimer(timestamp + interval)
    System.out.println("第 " + sdf.format(time) + " 分钟触发 ...")
    // |触发窗口操作时，调用窗口函数进行计算并保留窗口状态|
    TriggerResult.FIRE
  }

  // |当注册的事件时间定时器到达指定时间时调用此方法|
  override def onEventTime(time: Long, window: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    if (time == window.maxTimestamp) {
      System.out.println("水位线触发 ...")
      // |触发窗口操作时，调用窗口函数进行计算并保留窗口状态|
      TriggerResult.FIRE
    } else {
      TriggerResult.CONTINUE
    }
  }

  // |清除窗口状态|
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext) = {
    val fireTimestamp = ctx.getPartitionedState(processTimerStateDescriptor)
    ctx.deleteProcessingTimeTimer(fireTimestamp.get) // |清除处理时间定时器|
    fireTimestamp.clear() // |清除自定义状态中的触发时间|
    ctx.deleteEventTimeTimer(window.maxTimestamp) // |清除事件时间定时器|
  }

  // |更新状态时，使用新值替代旧值|
  class Update extends ReduceFunction[Long] {
    override def reduce(value1: Long, value2: Long): Long = value2
  }

}
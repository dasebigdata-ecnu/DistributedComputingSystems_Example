package cn.edu.ecnu.flink.examples.java.integersum.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.text.SimpleDateFormat;


public class CustomerTriggerWithAccumulation<W extends Window> extends Trigger<Object, W> {
  final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

  // |以自定义状态形式保存每个窗口中处理时间定时器所对应的触发时间|
  private final ReducingStateDescriptor<Long> processTimerStateDescriptor =
      new ReducingStateDescriptor<Long>(
          "processTimer", new CustomerTriggerWithAccumulation.Update(), LongSerializer.INSTANCE);

  // |基于处理时间触发间隔|
  Long interval = 60L;

  public CustomerTriggerWithAccumulation(long interval) {
    this.interval = interval * 1000;
  }

  // |当有记录进入相应窗口时触发器将调用此方法|
  @Override
  public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx)
      throws Exception {
    // |从自定义状态中获取处理时间定时器所对应的触发时间|
    ReducingState<Long> fireTimestamp = ctx.getPartitionedState(processTimerStateDescriptor);
    // |窗口中进入第一条记录，处理时间定时器所对应的触发时间状态不存在|
    if (fireTimestamp.get() == null) {
      Long timeStamp = ctx.getCurrentProcessingTime();
      // |计算窗口的下一次触发时间|
      Long start = timeStamp - (timeStamp % interval);
      Long nextFireTimestamp = start + interval;
      // |注册处理时间定时器|
      ctx.registerProcessingTimeTimer(nextFireTimestamp);
      // |将处理时间定时器所对应触发时间存入自定义状态|
      fireTimestamp.add(nextFireTimestamp);
    }
    // |迟到记录处理|
    if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
      System.out.println("迟到记录触发 ...");
      // |触发窗口操作时，调用窗口函数进行计算并保留窗口状态|
      return TriggerResult.FIRE;
    } else {
      // |根据记录所在窗口的最大时间戳，注册事件时间定时器|
      ctx.registerEventTimeTimer(window.maxTimestamp());
      // |对窗口不采取任何操作|
      return TriggerResult.CONTINUE;
    }
  }

  // |当注册的处理时间定时器到达指定时间时调用此方法|
  @Override
  public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
    ReducingState<Long> fireTimestamp = ctx.getPartitionedState(processTimerStateDescriptor);
    Long timestamp = fireTimestamp.get();
    // |更新自定义状态中的触发时间|
    fireTimestamp.add(timestamp + interval);
    // |根据窗口下一次触发的处理时间，注册处理时间定时器|
    ctx.registerProcessingTimeTimer(timestamp + interval);
    System.out.println("第 " + sdf.format(time) + " 分钟触发 ...");
    // |触发窗口操作时，调用窗口函数进行计算并保留窗口状态|
    return TriggerResult.FIRE;
  }

  // |当注册的事件时间定时器到达指定时间时调用此方法|
  @Override
  public TriggerResult onEventTime(long time, W window, TriggerContext triggerContext)
      throws Exception {
    if (time == window.maxTimestamp()) {
      System.out.println("水位线触发 ...");
      // |触发窗口操作时，调用窗口函数进行计算并清除窗口状态|
      return TriggerResult.FIRE;
    } else {
      return TriggerResult.CONTINUE;
    }
  }

  // |清除窗口状态|
  @Override
  public void clear(W window, TriggerContext ctx) throws Exception {
    ReducingState<Long> fireTimestamp = ctx.getPartitionedState(processTimerStateDescriptor);
    ctx.deleteProcessingTimeTimer(fireTimestamp.get()); // |清除处理时间定时器|
    fireTimestamp.clear(); // |清除自定义状态中的触发时间|
    ctx.deleteEventTimeTimer(window.maxTimestamp()); // |清除事件时间定时器|
  }

  // |更新状态时，使用新值替代旧值|
  private static class Update implements ReduceFunction<Long> {
    private static final long serialVersionUID = 1L;

    @Override
    public Long reduce(Long value1, Long value2) throws Exception {
      return value2;
    }
  }
}

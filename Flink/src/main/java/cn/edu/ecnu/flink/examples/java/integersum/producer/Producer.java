package cn.edu.ecnu.flink.examples.java.integersum.producer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Producer implements SourceFunction<Tuple2<String, Integer>> {
  boolean isBounded;
  final List<Tuple3<String, Integer, String>> data =
      new ArrayList<Tuple3<String, Integer, String>>(
          Arrays.asList(
              new Tuple3<String, Integer, String>("dataflow", 5, "12:00:30"),
              new Tuple3<String, Integer, String>("dataflow", 7, "12:02:30"),
              new Tuple3<String, Integer, String>("dataflow", 3, "12:03:45"),
              // 水位线
              new Tuple3<String, Integer, String>("dataflow", null, "12:02:00"),
              new Tuple3<String, Integer, String>("dataflow", 4, "12:03:50"),
              new Tuple3<String, Integer, String>("dataflow", 3, "12:04:30"),
              new Tuple3<String, Integer, String>("dataflow", 8, "12:03:30"),
              // 水位线
              new Tuple3<String, Integer, String>("dataflow", null, "12:04:00"),
              // 水位线
              new Tuple3<String, Integer, String>("dataflow", null, "12:06:00"),
              new Tuple3<String, Integer, String>("dataflow", 3, "12:06:30"),
              new Tuple3<String, Integer, String>("dataflow", 9, "12:01:30"),
              new Tuple3<String, Integer, String>("dataflow", 8, "12:07:30"),
              new Tuple3<String, Integer, String>("dataflow", 1, "12:07:50"),
              // 水位线
              new Tuple3<String, Integer, String>("dataflow", null, "12:08:00")));

  // 每个数据的处理时间间隔
  final List<Integer> processInterval =
      new ArrayList<Integer>(Arrays.asList(40, 15, 25, 10, 5, 15, 30, 10, 10, 30, 20, 40, 20, 20));

  final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

  public Producer(boolean isBounded) {
    this.isBounded = isBounded;
  }

  @Override
  public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
    waitForMinute();
    for (int i = 0; i < 14; i++) {
      // |记录发送延迟时间|
      Thread.sleep(processInterval.get(i) * 1000);
      Long timestamp = sdf.parse(data.get(i).f2).getTime();
      Integer value = data.get(i).f1;
      // |若为水位线记录且输入数据作为无界数据集，则生成系统的水位线|
      if (value == null) {
        if (!isBounded) {
          ctx.emitWatermark(new Watermark(sdf.parse(data.get(i).f2).getTime()));
        }
      }
      // |设置键值对的事件时间并发送至下游|
      else {
        ctx.collectWithTimestamp(new Tuple2<String, Integer>(data.get(i).f0, value), timestamp);
      }
    }
  }

  @Override
  public void cancel() {}

  private void waitForMinute() throws InterruptedException {
    Long interval = 60 * 1000L;
    Long timestamp = System.currentTimeMillis();
    Thread.sleep(interval - (timestamp % interval));
  }
}

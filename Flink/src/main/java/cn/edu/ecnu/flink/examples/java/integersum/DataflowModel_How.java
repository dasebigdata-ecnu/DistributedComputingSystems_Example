package cn.edu.ecnu.flink.examples.java.integersum;

import cn.edu.ecnu.flink.examples.java.integersum.trigger.CustomerTriggerWithAccumulation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import cn.edu.ecnu.flink.examples.java.integersum.producer.Producer;
import cn.edu.ecnu.flink.examples.java.integersum.trigger.CustomerTrigger;

import java.text.SimpleDateFormat;
import java.util.Iterator;

/** 使用Flink DataStream 实现基于事件时间并且带有水位线的窗口的聚合操作（允许数据存在延迟，并对延迟数据进行处理） */
public class DataflowModel_How {
  public static void main(String[] args) throws Exception {
    run(args);
  }

  public static void run(String[] args) throws Exception {
    /* |步骤1： 创建StreamExecutionEnvironment对象| */
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(1);

    /* |步骤2：按应用逻辑使用操作算子编写DAG，操作算子包括数据源、转换和数据池等| */
    DataStream<Tuple2<String, Integer>> source = env.addSource(new Producer(false));
    DataStream<String> sink =
        source
            .keyBy(0)
            .window(TumblingEventTimeWindows.of(Time.seconds(120L)))
            // |自定义触发器：在水位线机制的基础上，在处理时间域上每隔一分钟输出一次结果。同时，迟到数据修正窗口结果|
            .trigger(new CustomerTriggerWithAccumulation(60L))
            // |设置允许延迟时间为300s|
            .allowedLateness(Time.seconds(300L))
            .apply(new myWindowFunction());
    sink.print();

    /* |步骤3：触发程序执行| */
    env.execute("Dataflow Model-How");
  }

  static class myWindowFunction
      implements WindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow> {
    @Override
    public void apply(
        Tuple tuple,
        TimeWindow window,
        Iterable<Tuple2<String, Integer>> input,
        Collector<String> out)
        throws Exception {
      final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
      // |记录整数的累加和|
      int sum = 0;
      // |获取窗口中键值对的迭代器|
      Iterator<Tuple2<String, Integer>> it = input.iterator();
      // |遍历窗口中的键值对，并对整数进行求和|
      while (it.hasNext()) {
        Tuple2<String, Integer> next = it.next();
        sum = sum + next.f1;
      }
      // |以字符串形式返回形如”the sum of window [12:00:00,12:02:00) is 14”的窗口函数结果|
      String res =
          "the sum of window ["
              + sdf.format(window.getStart())
              + ","
              + sdf.format(window.getEnd())
              + ") is "
              + sum;
      out.collect(res);
    }
  }
}

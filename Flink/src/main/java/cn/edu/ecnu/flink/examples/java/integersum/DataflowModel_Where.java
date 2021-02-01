package cn.edu.ecnu.flink.examples.java.integersum;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import cn.edu.ecnu.flink.examples.java.integersum.producer.Producer;

import java.text.SimpleDateFormat;
import java.util.Iterator;

public class DataflowModel_Where {
  public static void main(String[] args) throws Exception {
    run(args);
  }

  public static void run(String[] args) throws Exception {
    /* |步骤1： 创建StreamExecutionEnvironment对象| */
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // |设置时间特征为事件时间|
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    /* |步骤2：按应用逻辑使用操作算子编写DAG，操作算子包括数据源、转换和数据池等| */
    DataStream<Tuple2<String, Integer>> source = env.addSource(new Producer(true));
    // |将source中的记录按照事件时间以2分钟为单位进行窗口划分|
    DataStream<String> sink =
        source
            .keyBy(0)
            .window(TumblingEventTimeWindows.of(Time.seconds(120L)))
            // |使用myWindowFunction作为窗口函数对整数进行累加求和|
            .apply(new myWindowFunction());
    sink.print();

    /* |步骤3：触发程序执行| */
    env.execute("Dataflow Model-Where");
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

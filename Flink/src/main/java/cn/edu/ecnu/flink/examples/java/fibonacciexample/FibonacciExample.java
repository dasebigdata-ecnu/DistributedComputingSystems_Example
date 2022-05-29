package cn.edu.ecnu.flink.examples.java.fibonacciexample;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FibonacciExample {
  public static void main(String[] args) throws Exception {
    run(args);
  }

  public static void run(String[] args) throws Exception {
    /* |步骤1： 创建StreamExecutionEnvironment对象| */
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /* |步骤2：按应用逻辑使用操作算子编写DAG，操作算子包括数据源、转换和数据池等| */
    // |接收来自Socket数据，创建名为inputStream的DataStream|
    DataStream<String> inputStream = env.socketTextStream("localhost", 9099, "\n");
    // |解析inputStream中的数据，创建名为first的DataStream|
    DataStream<Tuple3<Character, Long, Long>> first =
        inputStream.map(
            new MapFunction<String, Tuple3<Character, Long, Long>>() {
              @Override
              public Tuple3<Character, Long, Long> map(String value) throws Exception {
                return new Tuple3<>(
                    value.split(" ")[0].charAt(0),
                    Long.valueOf(value.split(" ")[1]),
                    Long.valueOf(value.split(" ")[2]));
              }
            });
    // |创建迭代算子|
    IterativeStream<Tuple3<Character, Long, Long>> iteration = first.iterate(5000L);
    // |实现迭代步逻辑，计算下一个斐波那契数|
    DataStream<Tuple3<Character, Long, Long>> iteratedStream =
        iteration.flatMap(
            new FlatMapFunction<Tuple3<Character, Long, Long>, Tuple3<Character, Long, Long>>() {
              @Override
              public void flatMap(
                  Tuple3<Character, Long, Long> value, Collector<Tuple3<Character, Long, Long>> out)
                  throws Exception {
                // |例如迭代算子的输入输入为(A, 1, 2)，此处转换将(A, 1, 2)转换为(A, 2, 3)|
                Tuple3<Character, Long, Long> feedbackValue =
                    new Tuple3(value.f0, value.f2, value.f1 + value.f2);
                // |例如迭代算子的输入输入为(A, 1, 2)，此处转换将(A, 1, 2)转换为(A, 1, Min)|
                Tuple3<Character, Long, Long> outputValue =
                    new Tuple3(value.f0, value.f1, Long.MIN_VALUE);
                out.collect(feedbackValue);
                out.collect(outputValue);
              }
            });
    // |创建反馈流|
    // |选择第三位置不为Min的元组，例如(A, 2, 3)|
    DataStream<Tuple3<Character, Long, Long>> feedback =
        iteratedStream.filter(
            new FilterFunction<Tuple3<Character, Long, Long>>() {
              @Override
              public boolean filter(Tuple3<Character, Long, Long> value) throws Exception {
                return value.f2 != Long.MIN_VALUE;
              }
            });
    iteration.closeWith(feedback);
    // |创建输出流|
    // |选择第三位置为Min的元组，例如(A, 1, 0)，并将其转换为(A, 1)|
    DataStream<Tuple2<Character, Long>> output =
        iteratedStream
            .filter(
                new FilterFunction<Tuple3<Character, Long, Long>>() {
                  @Override
                  public boolean filter(Tuple3<Character, Long, Long> value) throws Exception {
                    return value.f2 == Long.MIN_VALUE;
                  }
                })
            .map(
                new MapFunction<Tuple3<Character, Long, Long>, Tuple2<Character, Long>>() {
                  @Override
                  public Tuple2<Character, Long> map(Tuple3<Character, Long, Long> value)
                      throws Exception {
                    return new Tuple2<>(value.f0, value.f1);
                  }
                });
    // |输出流式迭代计算结果|
    output.print();

    /* |步骤3：触发程序执行| */
    env.execute("Streaming Iteration");
  }
}

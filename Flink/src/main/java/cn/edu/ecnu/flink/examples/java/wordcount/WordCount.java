package cn.edu.ecnu.flink.examples.java.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;

public class WordCount {
  public static void main(String[] args) throws Exception {
    run(args);
  }

  public static void run(String[] args) throws Exception {
    /* |步骤1：创建StreamExecutionEnvironment对象 |*/
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /* |步骤2：按应用逻辑使用操作算子编写DAG，操作算子包括数据源、转换、数据池等| */
    // |从指定的主机名和端口号接收数据，创建名为lines的DataStream|
    DataStream<String> lines = env.socketTextStream("localhost", 9099, "\n");
    // |将lines中的每一个文本行按空格分割成单个单词|
    DataStream<String> words =
        lines.flatMap(
            new FlatMapFunction<String, String>() {
              @Override
              public void flatMap(String value, Collector<String> out) throws Exception {
                for (String word : value.split(" ")) {
                  out.collect(word);
                }
              }
            });
    // |将每个单词的频数设置为1，即将每个单词映射为[单词, 1]|
    DataStream<Tuple2<String, Integer>> pairs =
        words.map(
            new MapFunction<String, Tuple2<String, Integer>>() {
              @Override
              public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
              }
            });
    // |按单词聚合，并对相同单词的频数使用sum进行累计|
    DataStream<Tuple2<String, Integer>> counts = pairs.keyBy(0).sum(1);
    // |输出词频统计结果|
    counts.print();

    /* |步骤3：触发程序执行| */
    env.execute("Streaming WordCount");
  }
}

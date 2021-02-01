package cn.edu.ecnu.sparkstreaming.examples.java.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import scala.Tuple2;

import java.util.*;

public class GlobalWordCount {
  public static void run(String[] args) throws InterruptedException {
    /* 步骤1：通过SparkConf设置配置信息，并创建StreamingContext */
    SparkConf conf =
        new SparkConf()
            .setAppName("GlobalWordCount")
            .setMaster("local[*]"); // 仅用于本地进行调试，如在集群中运行则删除该行
    JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

    // 若使用了有状态算子，则必须设置checkpoint
    ssc.checkpoint("hdfs://localhost:9000/spark/checkpoint");

    /* 步骤2：按应用逻辑使用操作算子编写DAG，包括DStream的输入、转换和输出等 */
    // 将接收到的文本行数据按空格分割
    JavaReceiverInputDStream<String> inputDStream = ssc.socketTextStream("localhost", 9999);

    // 将每个单词映射为[word, 1]键值对
    JavaPairDStream<String, Integer> pairsDStream =
        inputDStream
            .flatMap(
                new FlatMapFunction<String, String>() {
                  @Override
                  public Iterator<String> call(String line) throws Exception {
                    return Arrays.asList(line.split(" ")).iterator();
                  }
                })
            .mapToPair(
                new PairFunction<String, String, Integer>() {
                  @Override
                  public Tuple2<String, Integer> call(String word) throws Exception {
                    return new Tuple2<String, Integer>(word, 1);
                  }
                });

    // 使用updateStateByKey根据状态值和新到达数据统计词频
    DStream<Tuple2<String, Integer>> wordCounts =
        pairsDStream
            .updateStateByKey(
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                  @Override
                  public Optional<Integer> call(List<Integer> values, Optional<Integer> state)
                      throws Exception {
                    Integer updatedValue = 0;
                    if (state.isPresent()) {
                      updatedValue = state.get();
                    }
                    for (Integer value : values) {
                      updatedValue += value;
                    }
                    return Optional.of(updatedValue);
                  }
                })
            .checkpoint(Durations.seconds(25)); // 设置检查点间隔，最佳实践为批次间隔的5～10倍

    wordCounts.print(); // 打印结果

    /* 步骤3：开启计算并等待计算结束 */
    ssc.start();
    ssc.awaitTermination();
  }

  public static void main(String[] args) throws InterruptedException {
    run(args);
  }
}

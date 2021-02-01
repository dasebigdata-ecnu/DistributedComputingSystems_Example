package cn.edu.ecnu.sparkstreaming.examples.java.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;

public class BatchWordCount {
  public static void run(String[] args) throws InterruptedException {
    /* 步骤1：通过SparkConf设置配置信息，并创建StreamingContext */
    SparkConf sparkConf =
        new SparkConf()
            .setAppName("BatchWordCount")
            .setMaster("local[*]"); // 仅用于本地进行调试，如在集群中运行则删除该行
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

    /* 步骤2：按应用逻辑使用操作算子编写DAG，包括DStream的输入、转换和输出等 */
    // 从指定的主机名和端口号接收数据
    JavaReceiverInputDStream<String> inputDStream =
        ssc.socketTextStream("localhost", 9999);

    // 将接收到的文本行数据按空格分割
    JavaDStream<String> words =
        inputDStream.flatMap(
            new FlatMapFunction<String, String>() {
              @Override
              public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
              }
            });

    // 并将每个单词映射为[word, 1]键值对
    JavaPairDStream<String, Integer> mapToPairDStream =
        words.mapToPair(
            new PairFunction<String, String, Integer>() {
              @Override
              public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
              }
            });

    // 按单词聚合，对相同单词的频数进行累计
    JavaPairDStream<String, Integer> wordCounts =
        mapToPairDStream.reduceByKey(
            new Function2<Integer, Integer, Integer>() {
              @Override
              public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
              }
            });

    // 打印结果
    wordCounts.print();

    /* 步骤3：开启计算并等待计算结束 */
    ssc.start();
    ssc.awaitTermination();
  }

  public static void main(String[] args) throws InterruptedException {
    run(args);
  }
}

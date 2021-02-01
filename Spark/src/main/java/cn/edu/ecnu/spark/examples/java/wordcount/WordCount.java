package cn.edu.ecnu.spark.examples.java.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {

  public static void run(String[] args) {
    /* 步骤1：通过SparkConf设置配置信息，并创建SparkContext */
    SparkConf conf = new SparkConf();
    conf.setAppName("WordCount");
    conf.setMaster("local"); // 仅用于本地进行调试，如在集群中运行则删除本行
    JavaSparkContext sc = new JavaSparkContext(conf);

    /* 步骤2：按应用逻辑使用操作算子编写DAG，其中包括RDD的创建、转换和行动等 */
    // 读入文本数据，创建名为lines的RDD
    JavaRDD<String> lines = sc.textFile("src/main/resources/input/wordcount/words.txt");

    // 将lines中的每一个文本行按空格分割成单个单词
    JavaRDD<String> words =
        lines.flatMap(
            new FlatMapFunction<String, String>() {
              @Override
              public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
              }
            });
    // 将每个单词的频数设置为1，即将每个单词映射为[单词, 1]
    JavaPairRDD<String, Integer> pairs =
        words.mapToPair(
            new PairFunction<String, String, Integer>() {
              @Override
              public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
              }
            });
    // 按单词聚合，并对相同单词的频数使用sum进行累计
    JavaPairRDD<String, Integer> wordCounts =
        pairs
            .groupByKey()
            .mapToPair(
                new PairFunction<Tuple2<String, Iterable<Integer>>, String, Integer>() {
                  @Override
                  public Tuple2<String, Integer> call(Tuple2<String, Iterable<Integer>> t)
                      throws Exception {
                    Integer sum = Integer.valueOf(0);
                    for (Integer i : t._2) {
                      sum += i;
                    }
                    return new Tuple2<String, Integer>(t._1, sum);
                  }
                });
    // 合并机制
    /*JavaPairRDD<String, Integer> wordCounts =
    pairs.reduceByKey(
        new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer t1, Integer t2) throws Exception {
            return t1 + t2;
          }
        });*/

    // 输出词频统计结果
    wordCounts.foreach(t -> System.out.println(t._1 + " " + t._2));

    /* 步骤3：关闭SparkContext */
    sc.stop();
  }

  public static void main(String[] args) {
    run(args);
  }
}

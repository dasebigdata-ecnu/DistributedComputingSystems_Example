package cn.edu.ecnu.sparkstreaming.examples.java.anomaly;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class AnomalyDetection {
  public static void run(String[] args) throws InterruptedException {
    /* 步骤1：通过SparkConf设置配置信息，并创建StreamingContext */
    SparkConf sparkConf =
        new SparkConf()
            .setAppName("AnomalyDetection")
            .setMaster("local[*]"); // 仅用于本地进行调试，如在集群中运行则删除该行
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

    /* 步骤2：按应用逻辑使用操作算子编写DAG，包括DStream的输入、转换和输出等 */
    // 模型参数
    Double w = 1.1;
    Double b = 2.2;
    Double delta = 0.5;

    // 从指定的主机名和端口号接收数据
    JavaReceiverInputDStream<String> inputDStream = ssc.socketTextStream("localhost", 9999);

    // 按逗号分割解析每行数据，并转化为Double类型
    JavaPairDStream<Double, Double> anomaly =
        inputDStream
            .mapToPair(
                new PairFunction<String, Double, Double>() {
                  @Override
                  public Tuple2<Double, Double> call(String line) throws Exception {
                    String[] tokens = line.split(",");
                    return new Tuple2<>(Double.valueOf(tokens[0]), Double.valueOf(tokens[1]));
                  }
                })
            .filter(
                new Function<Tuple2<Double, Double>, Boolean>() { // 使用线性模型检测异常
                  @Override
                  public Boolean call(Tuple2<Double, Double> t) throws Exception {
                    return Math.abs(w * t._1 + b - t._2) > delta;
                  }
                });

    // 输出异常
    anomaly.print();

    /* 步骤3：开启计算并等待计算结束 */
    ssc.start();
    ssc.awaitTermination();
  }

  public static void main(String[] args) throws InterruptedException {
    run(args);
  }
}

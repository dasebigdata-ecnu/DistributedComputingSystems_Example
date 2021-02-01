package cn.edu.ecnu.sparkstreaming.examples.scala.anomaly

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AnomalyDetection {
  def run(args: Array[String]): Unit = {
    /* 步骤1：通过SparkConf设置配置信息，并创建StreamingContext */
    val sparkConf = new SparkConf()
      .setAppName("AnomalyDetection")
      .setMaster("local[*]") // 仅用于本地进行调试，如在集群中运行则删除该行
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    /* 步骤2：按应用逻辑使用操作算子编写DAG，包括DStream的输入、转换和输出等 */
    // 模型参数
    val w = 1.1
    val b = 2.2
    val delta = 0.5

    // 从指定的主机名和端口号接收数据
    val inputDStream = ssc.socketTextStream("localhost", 9999)

    // 按逗号分割解析每行数据，并转化为Double类型
    val anomaly = inputDStream
      .map(line => {
        val tokens = line.split(",")
        (tokens(0).toDouble, tokens(1).toDouble)
      })
      // 使用线性模型检测异常
      .filter(t => {
        Math.abs(w * t._1 + b - t._2) > delta
      })

    // 输出异常
    anomaly.print()

    /* 步骤3：开启计算并等待计算结束 */
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}

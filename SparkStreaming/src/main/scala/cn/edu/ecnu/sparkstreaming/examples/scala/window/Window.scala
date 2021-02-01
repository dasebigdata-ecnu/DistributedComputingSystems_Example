package cn.edu.ecnu.sparkstreaming.examples.scala.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Window {
  def run(args: Array[String]): Unit = {
    /* 步骤1：通过SparkConf设置配置信息，并创建StreamingContext */
    val sparkConf = new SparkConf()
      .setAppName("Window")
      .setMaster("local[*]") // 仅用于本地进行调试，如在集群中运行则删除该行
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 如需使用增量式窗口操作则必须设置检查点路径
    // ssc.checkpoint("hdfs://localhost:9000/sparkstreaming/checkpoint")

    /* 步骤2：按应用逻辑使用操作算子编写DAG，包括DStream的输入、转换和输出等 */
    // 从指定的主机名和端口号接收数据
    val inputDStream = ssc.socketTextStream("localhost", 9999)

    // 将接收到的文本行数据按空格分割，并将每个单词映射为[word, 1]键值对
    val pairsDStream = inputDStream.flatMap(_.split(" ")).map(x => (x, 1))

    // 按单词聚合，对相同单词的频数进行累计
    val wordCounts = pairsDStream.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(10), Seconds(5))
    // 如需使用增量式窗口操作则将上一行替换为下行
    // val wordCounts = pairsDStream.reduceByKeyAndWindow(_+_, _-_, Seconds(10), Seconds(5)).checkpoint(Seconds(25))

    // 打印结果
    wordCounts.print()

    /* 步骤3：开启计算并等待计算结束 */
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}

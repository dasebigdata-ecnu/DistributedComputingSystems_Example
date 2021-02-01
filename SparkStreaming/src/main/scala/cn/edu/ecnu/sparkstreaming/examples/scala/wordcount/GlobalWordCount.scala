package cn.edu.ecnu.sparkstreaming.examples.scala.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object GlobalWordCount {
  def run(args: Array[String]): Unit = {
    /* 步骤1：通过SparkConf设置配置信息，并创建StreamingContext */
    val conf = new SparkConf()
      .setAppName("GlobalWordCount")
      .setMaster("local[*]") // 仅用于本地进行调试，如在集群中运行则删除该行
    val ssc = new StreamingContext(conf, Seconds(5))

    // 若使用了有状态算子，则必须设置checkpoint
    ssc.checkpoint("hdfs://localhost:9000/sparkstreaming/checkpoint")

    /* 步骤2：按应用逻辑使用操作算子编写DAG，包括DStream的输入、转换和输出等 */
    // 从指定的主机名和端口号接收数据
    val inputDStream = ssc.socketTextStream("localhost", 9999)

    // 将接收到的文本行数据按空格分割，并将每个单词映射为[word, 1]键值对
    val pairsDStream = inputDStream.flatMap(_.split(" ")).map(word => (word, 1))

    // 使用updateStateByKey根据状态值和新到达数据统计词频
    val wordCounts = pairsDStream.updateStateByKey(
      (curValues: Seq[Int], preValue: Option[Int]) => {
        val curValue = curValues.sum
        Some(curValue + preValue.getOrElse(0))
      })
      .checkpoint(Seconds(25)) // 设置检查点间隔，最佳实践为批次间隔的5～10倍

    wordCounts.print() // 打印结果

    /* 步骤3：开启计算并等待计算结束 */
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}

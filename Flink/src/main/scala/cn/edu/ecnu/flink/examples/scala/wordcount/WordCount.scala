package cn.edu.ecnu.flink.examples.scala.wordcount

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WordCount {
  def run(args: Array[String]): Unit = {
    /* |步骤1：创建StreamExecutionEnvironment对象 |*/
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /* |步骤2：按应用逻辑使用操作算子编写DAG，操作算子包括数据源、转换、数据池等| */
    // |从指定的主机名和端口号接收数据，创建名为lines的DataStream|
    val lines = env.socketTextStream("localhost", 9099)
    // |将lines中的每一个文本行按空格分割成单个单词|
    val words = lines.flatMap(w => w.split(" "))
    // |将每个单词的频数设置为1，即将每个单词映射为[单词, 1]|
    val pairs = words.map(word => (word, 1))
    // |按单词聚合，并对相同单词的频数使用sum进行累计|
    val counts = pairs.keyBy(0)
      .sum(1)
    // |输出词频统计结果|
    counts.print()

    /* |步骤3：触发程序执行| */
    env.execute("Streaming WordCount")
  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}

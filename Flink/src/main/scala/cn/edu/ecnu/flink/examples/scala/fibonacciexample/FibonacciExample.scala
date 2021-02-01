package cn.edu.ecnu.flink.examples.scala.fibonacciexample

import java.util.Random

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


object FibonacciExample {
  def run(args: Array[String]): Unit = {
    /* |步骤1： 创建StreamExecutionEnvironment对象| */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    /* |步骤2：按应用逻辑使用操作算子编写DAG，操作算子包括数据源、转换和数据池等| */
    // |接收来自Socket数据，创建名为inputStream的DataStream|
    val inputStream = env.socketTextStream("localhost", 9099)
    // |解析inputStream中的数据，创建名为first的DataStream|
    val first: DataStream[(Char, Long, Long)] = inputStream.map(lines => (lines.split(" ")(0).charAt(0),
      lines.split(" ")(1).toLong, lines.split(" ")(2).toLong))
    val outputStream = first
      // |创建迭代算子|
      .iterate(
        (iteration: DataStream[(Char, Long, Long)]) => {
          // |实现迭代步逻辑，计算下一个斐波那契数|
          val step = iteration.flatMap(t => {
            // |例如迭代算子的输入输入为(A, 1, 2)，此处转换将(A, 1, 2)转换为(A, 2, 3)|
            val feedbackValue = (t._1, t._3, t._2 + t._3)
            // |例如迭代算子的输入输入为(A, 1, 2)，此处转换将(A, 1, 2)转换为(A, 1, Min)|
            val outputValue = (t._1, t._2, Long.MinValue)
            val list = feedbackValue :: outputValue :: Nil
            list.toIterator
          }).setParallelism(2)
          // |创建反馈流|
          // |选择第三位置不为Min的元组，例如(A, 2, 3)|
          val feedback = step.filter(_._3 != Long.MinValue)
          // |创建输出流|
          // |选择第三位置为Min的元组，例如(A, 1, 0)，并将其转换为(A, 1)|
          val output = step.filter(_._3 == Long.MinValue).map(t => (t._1, t._2))
          (feedback, output)
        }
        // |设置等待反馈输入的最大时间间隔为5s|
        , 5000L
      )
    // |输出流式迭代计算结果|
    outputStream.print()

    /* |步骤3：触发程序执行| */
    env.execute("Streaming Iteration")
  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}

package cn.edu.ecnu.flink.examples.scala.wordcountwithfaulttolerance

import org.apache.flink.streaming.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object WordCountWithFaultTolerance {
  def run(args: Array[String]): Unit = {
    /* |步骤1： 创建StreamExecutionEnvironment对象| */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // |设置checkpoint的周期，每隔1000ms试图启动一个检查点|
    env.enableCheckpointing(1000)
    // |设置检查点的最大并发数|
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
    // |设置statebackend，使用FsStateBackend将状态存储至hdfs|
    env.setStateBackend(new FsStateBackend("hdfs://hadoop:9000/flink/checkpoints"))
    // |处理程序被cancel后，会保留checkpoint数据|
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig
      .ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    /* |步骤2：按应用逻辑使用操作算子编写DAG，操作算子包括数据源、转换和数据池等| */
    val lines: DataStream[String] = env.socketTextStream("localhost", 9099)
    val words = lines.flatMap(w => w.split(" "))
    val pairs: DataStream[(String, Int)] = words.map(word => (word, 1))
    val counts: DataStream[(String, Int)] = pairs
      .keyBy(0)
      .sum(1)
    counts.print()

    /* |步骤3：触发程序执行| */
    env.execute("WordCount With Fault Tolerance")
  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}

package cn.edu.ecnu.flink.examples.scala.integersum

import cn.edu.ecnu.flink.examples.scala.integersum.producer.Producer
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object DataflowModel_What {
  def run(args: Array[String]): Unit = {
    /* |步骤1： 创建StreamExecutionEnvironment对象| */
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /* |步骤2：按应用逻辑使用操作算子编写DAG，操作算子包括数据源、转换和数据池等| */
    // |接收来自CustomSource的记录，抛弃代表watermark的记录，创建名为source的DataStream|
    val source = env.addSource(new Producer(true))
    // |对键值对按键聚合，并使用sum对整数进行累加，创建名为sink的DataStream|
    val sink = source.keyBy(0).sum(1)
    // |输出整数求和结果|
    sink.print()

    /* |步骤3：触发程序执行| */
    env.execute("Dataflow Model-What")
  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}

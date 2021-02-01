package cn.edu.ecnu.flink.examples.java.integersum;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import cn.edu.ecnu.flink.examples.java.integersum.producer.Producer;

public class DataflowModel_What {
  public static void main(String[] args) throws Exception {
    run(args);
  }

  private static void run(String[] args) throws Exception {
    /* |步骤1： 创建StreamExecutionEnvironment对象| */
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /* |步骤2：按应用逻辑使用操作算子编写DAG，操作算子包括数据源、转换和数据池等| */
    // |接收来自CustomSource的记录，抛弃代表watermark的记录，创建名为source的DataStream|
    DataStream<Tuple2<String, Integer>> source = env.addSource(new Producer(true));
    // |对键值对按键聚合，并使用sum对整数进行累加，创建名为sink的DataStream|
    DataStream<Tuple2<String, Integer>> sink = source.keyBy(0).sum(1);
    // |输出整数求和结果|
    sink.print();

    /* |步骤3：触发程序执行| */
    env.execute("Dataflow Model-What");
  }
}

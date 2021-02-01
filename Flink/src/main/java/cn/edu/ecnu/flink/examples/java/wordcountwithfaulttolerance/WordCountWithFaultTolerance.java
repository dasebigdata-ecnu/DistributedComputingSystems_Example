package cn.edu.ecnu.flink.examples.java.wordcountwithfaulttolerance;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountWithFaultTolerance {
  public static void main(String[] args) throws Exception {
    run(args);
  }

  public static void run(String[] args) throws Exception {
    /* |步骤1：创建StreamExecutionEnvironment对象 |*/
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // |设置checkpoint的周期，每隔1000ms尝试启动一个检查点|
    env.enableCheckpointing(1000);
    // |设置检查点的最大并发数|
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(Integer.MAX_VALUE);
    // |设置statebackend，使用FsStateBackend将状态存储至hdfs|
    env.setStateBackend(new FsStateBackend("hdfs://hadoop:9000/flink/checkpoints"));
    // |处理程序被cancel后，会保留checkpoint数据|
    env.getCheckpointConfig()
        .enableExternalizedCheckpoints(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    /* |步骤2：按应用逻辑使用操作算子编写DAG，操作算子包括数据源、转换、数据池等| */
    DataStream<String> lines = env.socketTextStream("localhost", 9099, "\n");
    DataStream<String> words =
        lines.flatMap(
            new FlatMapFunction<String, String>() {
              @Override
              public void flatMap(String value, Collector<String> out) throws Exception {
                for (String word : value.split(" ")) {
                  out.collect(word);
                }
              }
            });
    DataStream<Tuple2<String, Integer>> pairs =
        words.map(
            new MapFunction<String, Tuple2<String, Integer>>() {
              @Override
              public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
              }
            });
    DataStream<Tuple2<String, Integer>> counts = pairs.keyBy(0).sum(1);
    counts.print();

    /* |步骤3：触发程序执行| */
    env.execute("WordCount With Fault Tolerance");
  }
}

package cn.edu.ecnu.example.storm.wordcount.withAck;

import cn.edu.ecnu.example.storm.wordcount.CountBolt;
import cn.edu.ecnu.example.storm.wordcount.SplitBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopologyWithAck {
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.exit(-1);
      return;
    }
    /* 步骤1：构建拓扑 */
    TopologyBuilder builder = new TopologyBuilder();
    // 设置Spout，这个Spout的名字叫做"SPOUT"，executor数量为1，任务数量为1
    builder.setSpout("SPOUT", new SocketSpoutWithAck(args[1], args[2]), 1);
    // 设置Bolt——“SPLIT”，executor数量为2，任务数量为2，与"SPOUT"之间的流分组策略为随机分组
    builder.setBolt("SPLIT", new SplitBolt(), 2).setNumTasks(2).shuffleGrouping("SPOUT");
    // 设置Bolt——“COUNT”,executor数量为2，任务数量为2，订阅策略为fieldsGrouping
    builder.setBolt("COUNT", new CountBolt(), 2).fieldsGrouping("SPLIT", new Fields("word"));

    /* 步骤2：设置配置信息 */
    Config conf = new Config();
    conf.setDebug(false); // 关闭调试模式
    conf.setNumWorkers(2); // 设置Worker数量为2
    conf.setNumAckers(2); // 设置Acker数量为0

    /* 步骤3：指定程序运行的方式 */
    if (args[0].equals("cluster")) { // 在集群运行程序，拓扑的名称为WORDCOUNTwithack
      StormSubmitter.submitTopology("WORDCOUNTwithack", conf, builder.createTopology());
    } else if (args[0].equals("local")) {
      // 在本地IDE调试程序，拓扑的名称为WORDCOUNTwithack
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("WORDCOUNTwithack", conf, builder.createTopology());
    } else {
      System.exit(-2);
    }
  }
}

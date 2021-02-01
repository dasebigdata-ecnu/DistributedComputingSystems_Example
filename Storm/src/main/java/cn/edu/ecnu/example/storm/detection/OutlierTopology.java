package cn.edu.ecnu.example.storm.detection;

import cn.edu.ecnu.example.storm.common.SocketSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class OutlierTopology {
  public static void main(String[] args)
      throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
    if (args.length < 3) {
      System.exit(-1);
      return;
    }

    /* 步骤1:构建拓扑 */
    TopologyBuilder builder = new TopologyBuilder();
    // 设置Spout，这个Spout的名字叫做"SPOUT"，设置并行度为1
    builder.setSpout("SPOUT", new SocketSpout(args[1], args[2]), 1);
    // 设置Bolt——“split”，并行度为2，它的数据来源是spout的
    builder
        .setBolt("DETECTION", new DetectionBolt(1.5, 2.5, 0.5), 2)
        .setNumTasks(2)
        .shuffleGrouping("SPOUT");

    /* 步骤2:设置配置信息 */
    Config conf = new Config();
    conf.setDebug(false);
    conf.setNumWorkers(2);
    conf.setNumAckers(0);

    /* 步骤3:指定运行程序的方式 */
    if (args[0].equals("cluster")) { // 在集群运行程序,拓扑名称为OUTLIERTOPOLOGY
      conf.setNumWorkers(2);
      conf.setNumAckers(2);
      StormSubmitter.submitTopology("OUTLIERTOPOLOGY", conf, builder.createTopology());
    } else if (args[0].equals("local")) {
      // 在本地IDE调试程序,拓扑的名称为OUTLIERTOPOLOGY
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("OUTLIERTOPOLOGY", conf, builder.createTopology());
    } else {
      System.exit(-2);
    }
  }
}

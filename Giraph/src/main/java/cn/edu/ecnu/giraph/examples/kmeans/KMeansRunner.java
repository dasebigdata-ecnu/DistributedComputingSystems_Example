package cn.edu.ecnu.giraph.examples.kmeans;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.io.formats.GiraphTextInputFormat;
import org.apache.giraph.io.formats.GiraphTextOutputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.TextDoubleDoubleAdjacencyListVertexInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KMeansRunner extends Configured implements Tool {

  // 聚类中心集的名称
  private static final String CENTERS = "centers";

  @Override
  public int run(String[] args) throws Exception {
    /* 步骤1: 设置作业的信息 */
    GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());

    // 设置compute方法
    giraphConf.setComputationClass(KMeansComputation.class);
    // 设置图数据的输入格式
    giraphConf.setVertexInputFormatClass(TextDoubleDoubleAdjacencyListVertexInputFormat.class);
    // 设置图数据的输出格式
    giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    // 设置MasterCompute，启用Aggregator机制
    giraphConf.setMasterComputeClass(KMeansMasterCompute.class);
    // 设置初始聚类中心集的文件路径的配置项
    giraphConf.set(CENTERS, args[2]);

    // 启用本地调试模式
    giraphConf.setLocalTestMode(true);
    // 最小的Worker数量和最大的Worker数量均为1，Master协调超步时所需Worker响应的百分比为100
    giraphConf.setWorkerConfiguration(1, 1, 100);
    // Master和Worker位于同一进程
    GiraphConstants.SPLIT_MASTER_WORKER.set(giraphConf, false);

    // 创建Giraph作业
    GiraphJob giraphJob = new GiraphJob(giraphConf, getClass().getSimpleName());

    // 设置图数据的输入路径
    GiraphTextInputFormat.addVertexInputPath(giraphConf, new Path(args[0]));
    // 设置图数据的输出路径
    GiraphTextOutputFormat.setOutputPath(giraphJob.getInternalJob(), new Path(args[1]));

    return giraphJob.run(true) ? 0 : -1;
  }

  public static void main(String[] args) throws Exception {
    /* 步骤2: 运行作业 */
    int exitCode = ToolRunner.run(new KMeansRunner(), args);
    System.exit(exitCode);
  }
}

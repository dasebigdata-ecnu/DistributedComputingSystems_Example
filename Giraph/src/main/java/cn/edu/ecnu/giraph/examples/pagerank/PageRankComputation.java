package cn.edu.ecnu.giraph.examples.pagerank;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

/* 步骤1：确定顶点标识I、顶点的计算值V、边的权值E以及消息值M的数据类型 */
public class PageRankComputation
    extends BasicComputation<Text, DoubleWritable, DoubleWritable, DoubleWritable> {

  // 阻尼系数
  private static final double D = 0.85;
  // 最大超步数
  private static final int MAX_ITERATION = 20;

  @Override
  public void compute(Vertex<Text, DoubleWritable, DoubleWritable> vertex,
      Iterable<DoubleWritable> messages) {
    /* 步骤2：编写与顶点计算、更新相关的处理逻辑以及发送消息 */
    if (getSuperstep() > 0) {
      // 对接收到的贡献值进行累加
      double sum = 0;
      for (DoubleWritable message : messages) {
        sum += message.get();
      }
      // |根据公式计算并更新排名值|
      double rankValue = (1 - D) / getTotalNumVertices() + D * sum;
      vertex.setValue(new DoubleWritable(rankValue));
    }

    // 小于设定的最大超步数则发送消息，否则使得顶点进入非活跃状态
    if (getSuperstep() < MAX_ITERATION) {
      // 存在出站链接时，各网页将网页的贡献值发送给链向的网页
      if (vertex.getNumEdges() != 0) {
        sendMessageToAllEdges(
            vertex, new DoubleWritable(vertex.getValue().get() / vertex.getNumEdges()));
      }
    } else {
      // 将当前网页的排名值四舍五入保留5位小数
      double rankValue = vertex.getValue().get();
      rankValue = Double.parseDouble(String.format("%.5f", rankValue));
      vertex.setValue(new DoubleWritable(rankValue));
      vertex.voteToHalt();
    }
  }
}

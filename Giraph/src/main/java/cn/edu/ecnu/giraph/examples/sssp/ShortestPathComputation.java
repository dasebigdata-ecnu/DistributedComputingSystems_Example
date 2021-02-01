package cn.edu.ecnu.giraph.examples.sssp;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

/* 步骤1：确定顶点标识I、顶点的计算值V、边的权值E以及消息值M的数据类型 */
public class ShortestPathComputation
    extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

  // 源点
  protected static final int SOURCE_VERTEX = 0;
  // 表示无穷大
  protected static final Double INF = Double.MAX_VALUE;

  @Override
  public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
      Iterable<DoubleWritable> messages) {
    /* 步骤2：编写与顶点计算、更新相关的处理逻辑以及发送消息 */
    // 超步0时将顶点初始化为表示无穷大的INF
    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(INF));
    }

    // 根据接收到的消息计算当前距离源点的最短路径值
    double minDist = vertex.getId().get() == SOURCE_VERTEX ? 0d : INF;
    for (DoubleWritable message : messages) {
      minDist = Math.min(minDist, message.get());
    }

    // 当minDist小于顶点的计算值时将计算值更新为minDist
    if (minDist < vertex.getValue().get()) {
      vertex.setValue(new DoubleWritable(minDist));
      for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
        double distance = minDist + edge.getValue().get();
        sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
      }
    }

    vertex.voteToHalt();
  }
}

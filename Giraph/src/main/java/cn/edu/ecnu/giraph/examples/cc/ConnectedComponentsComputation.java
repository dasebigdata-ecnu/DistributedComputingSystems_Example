package cn.edu.ecnu.giraph.examples.cc;

import java.io.IOException;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

public class ConnectedComponentsComputation
    extends BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {

  @Override
  public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex,
      Iterable<IntWritable> messages) throws IOException {
    /* 步骤2：编写与顶点计算、更新相关的处理逻辑以及发送消息 */
    // 超步0时向所有邻居顶点发送消息
    if (getSuperstep() == 0) {
      sendMessageToAllEdges(vertex, vertex.getValue());
      vertex.voteToHalt();
      return;
    }

    boolean changed = false;
    int currentComponent = vertex.getValue().get();
    // 从消息中挑选出最小的连通分量编号
    for (IntWritable message : messages) {
      int candidateComponent = message.get();
      if (candidateComponent < currentComponent) {
        currentComponent = candidateComponent;
        changed = true;
      }
    }

    if (changed) {
      // 更新计算值并向邻居顶点发送消息
      vertex.setValue(new IntWritable(currentComponent));
      sendMessageToAllEdges(vertex, vertex.getValue());
    }
    vertex.voteToHalt();
  }
}

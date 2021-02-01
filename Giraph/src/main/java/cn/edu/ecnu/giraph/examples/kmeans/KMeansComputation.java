package cn.edu.ecnu.giraph.examples.kmeans;

import cn.edu.ecnu.giraph.examples.kmeans.utils.PointsOperation;
import java.util.ArrayList;
import java.util.List;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

/* 步骤1：确定顶点标识I、顶点的计算值V、边的权值E以及消息值M的数据类型 */
public class KMeansComputation
    extends BasicComputation<Text, DoubleWritable, DoubleWritable, NullWritable> {
  // 最大超步数
  private static final int MAX_SUPERSTEP = 20;
  // 聚类中心集的名称
  private static final String CENTERS = "centers";
  // 聚类中心1和2的名称前缀
  private static final String CENTER_PREFIX = "center";

  @Override
  public void compute(Vertex<Text, DoubleWritable, DoubleWritable> vertex,
      Iterable<NullWritable> iterable) {
    /* 步骤2：编写与顶点计算、更新相关的处理逻辑以及发送消息 */
    if (getSuperstep() < MAX_SUPERSTEP) {
      // 通过Aggregator获取聚类中心集并进行解析
      String centersStr = getAggregatedValue(CENTERS).toString();
      List<List<Double>> centers = PointsOperation.parse(centersStr);

      // 解析顶点中保存的数据点
      List<Double> point = new ArrayList<>();
      for (String dimension : vertex.getId().toString().split(",")) {
        point.add(Double.parseDouble(dimension));
      }

      // 遍历聚类中心集并计算与数据点的距离
      double minDistance = Double.MAX_VALUE;
      int centerIndex = -1;

      for (int i = 0; i < centers.size(); i++) {
        double distance = 0;
        List<Double> center = centers.get(i);
        for (int j = 0; j < center.size(); j++) {
          distance += Math.pow(point.get(j) - center.get(j), 2);
        }

        distance = Math.sqrt(distance);
        if (distance < minDistance) {
          minDistance = distance;
          centerIndex = i + 1;
        }
      }
      vertex.setValue(new DoubleWritable(centerIndex));
      // 将数据点提供给所属聚类中心的Aggregator
      aggregate(CENTER_PREFIX + centerIndex,
          new Text(StringUtils.join(",", point) + "\t"));
    } else {
      vertex.voteToHalt();
    }
  }
}

package cn.edu.ecnu.giraph.examples.kmeans;

import cn.edu.ecnu.giraph.examples.kmeans.utils.PointsOperation;
import java.util.ArrayList;
import java.util.List;
import org.apache.giraph.aggregators.TextAppendAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

public class KMeansMasterCompute extends DefaultMasterCompute {

  // 聚类中心个数
  private static final int CENTER_SIZE = 2;
  // 最大超步数
  private static final int MAX_SUPERSTEP = 20;
  // 聚类中心集的名称
  private static final String CENTERS = "centers";
  // 聚类中心1和2的名称前缀
  private static final String CENTER_PREFIX = "center";

  @Override
  public void initialize() throws InstantiationException, IllegalAccessException {
    /* 步骤1：注册Aggregator */
    // 注册聚类中心集的Aggregator
    registerAggregator(CENTERS, TextAppendAggregator.class);
    for (int i = 1; i <= CENTER_SIZE; i++) {
      registerAggregator(CENTER_PREFIX + i, TextAppendAggregator.class);
    }
  }

  @Override
  public void compute() {
    /* 步骤2：对汇总的数据进行处理 */
    StringBuilder centers = new StringBuilder();
    // 超步0时从文件中读取聚类中心集
    if (getSuperstep() == 0) {
      String centersPath = getConf().get(CENTERS);
      for (String center : PointsOperation.getCenters(centersPath)) {
        centers.append(center).append("\t");
      }
    } else if (getSuperstep() < MAX_SUPERSTEP) {
      // 依次处理聚类中心1和2的Aggregator汇总的数据点
      for (int i = 1; i <= CENTER_SIZE; i++) {
        List<Double> newCenter = new ArrayList<>();
        // 获取并解析出属于同一聚类中心的数据点
        String datas = getAggregatedValue(CENTER_PREFIX + i).toString();
        List<List<Double>> points = PointsOperation.parse(datas);
        // 计算每个维度的平均值从而得到新的聚类中心
        for (int j = 0; j < points.get(0).size(); j++) {
          double sum = 0;
          for (List<Double> point : points) {
            sum += point.get(j);
          }
          newCenter.add(sum / points.size());
        }
        centers.append(StringUtils.join(",", newCenter)).append("\t");
      }
    }
    // 汇总聚类中心到Aggregator
    setAggregatedValue(CENTERS, new Text(centers.toString()));
  }
}

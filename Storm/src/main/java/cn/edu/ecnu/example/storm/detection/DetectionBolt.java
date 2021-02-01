package cn.edu.ecnu.example.storm.detection;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class DetectionBolt extends BaseBasicBolt {

  double w, b, delta; // 线性模型参数

  public DetectionBolt(double w, double b, double delta) {
    this.w = w;
    this.b = b;
    this.delta = delta;
  }

  /* 步骤1:定义元组处理逻辑 */
  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    // 获取数据
    double x = tuple.getDoubleByField("x");
    double y = tuple.getDoubleByField("y");
    // 判断数据是否异常
    if (Math.abs(w * x + b - y) > delta) {
      System.out.println(x + " " + y);
    }
  }

  /* 步骤2:声明输出元组的字段名称 */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    // 为空
  }
}

package cn.edu.ecnu.example.storm.wordcount;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.StringTokenizer;

public class SplitBolt extends BaseBasicBolt {
  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
  }
  /* 步骤1：描述元组的处理逻辑 */
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String sentence = tuple.getStringByField("sentence");
    StringTokenizer iter = new StringTokenizer(sentence);
    while (iter.hasMoreElements()) {
      collector.emit(new Values(iter.nextToken()));
    }
  }

  /* 步骤2：声明输出元组的字段名称 */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // 该元组仅有一个字段
    declarer.declare(new Fields("word"));
  }
}

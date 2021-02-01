package cn.edu.ecnu.example.storm.wordcount;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseBasicBolt {
  // 保存单词的频数
  Map<String, Integer> counts = new HashMap<String, Integer>();

  /* 步骤1：描述元组的处理逻辑 */
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    // 从接收到的元组中按字段提取单词
    String word = tuple.getStringByField("word");
    // 获取该单词对应的频数
    Integer count = counts.get(word);
    if (count == null) {
      count = 0;
    }
    // 计数增加，并将单词和对应的频数加入 map 中
    count++;
    counts.put(word, count);
    // 输出结果，也可采用写入文件等其它方式
    System.out.println(word + "," + count);
  }

  /* 步骤2：声明输出元组的字段名称 */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // 为空
  }
}

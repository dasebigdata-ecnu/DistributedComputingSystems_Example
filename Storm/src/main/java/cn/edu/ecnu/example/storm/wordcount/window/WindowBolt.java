package cn.edu.ecnu.example.storm.wordcount.window;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WindowBolt extends BaseBasicBolt {

  // 窗口的元组
  private final List<String> window = new ArrayList<>();
  // 窗口的大小和间隔
  private static final int LENGTH_AND_INTERVAL = 3;

  /* 步骤1：描述元组的处理逻辑 */
  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    // 缓存接收到的单词元组
    String word = tuple.getStringByField("word");
    window.add(word);
    // 接收的单词元组数量等于窗口间隔时，触发计数操作
    if (window.size() == LENGTH_AND_INTERVAL) {
      // 计数
      Map<String, Integer> wordCounts = new HashMap<>();
      for (String wordInWindow : window) {
        if (wordCounts.containsKey(wordInWindow)) {
          wordCounts.put(wordInWindow, wordCounts.get(wordInWindow) + 1);
        } else {
          wordCounts.put(wordInWindow, 1);
        }
      }

      // 输出计数结果
      for (Entry<String, Integer> entry : wordCounts.entrySet()) {
        System.out.println(entry.getKey() + " " + entry.getValue());
      }

      // 清除窗口内容
      window.clear();
    }
  }

  /* 步骤2：声明输出元组的字段名称 */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // 为空
  }
}

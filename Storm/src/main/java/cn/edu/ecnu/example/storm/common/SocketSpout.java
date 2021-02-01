package cn.edu.ecnu.example.storm.common;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Map;

public class SocketSpout extends BaseRichSpout {
  SpoutOutputCollector collector;
  String ip;
  int port;
  BufferedReader br = null;
  Socket socket = null;

  public SocketSpout(String ip, String port) {
    this.ip = ip;
    this.port = Integer.parseInt(port);
  }

  /* 步骤1: 初始化Spout */
  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
    this.collector = collector;
    try {
      socket = new Socket(ip, port);
      br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    try {
      br.close();
      socket.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /* 步骤2：读取并发送元组 */
  @Override
  public void nextTuple() {
    try {
      String tuple;
      if ((tuple = br.readLine()) != null) { // 读取元组
        collector.emit(new Values(tuple)); // 发送元组
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /* 步骤3：声明输出元组的字段名称 */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // 该输出元组仅有一个字段sentence
    declarer.declare(new Fields("sentence"));
  }
}

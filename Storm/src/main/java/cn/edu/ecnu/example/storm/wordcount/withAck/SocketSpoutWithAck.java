package cn.edu.ecnu.example.storm.wordcount.withAck;

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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SocketSpoutWithAck extends BaseRichSpout {
  SpoutOutputCollector collector;
  String ip;
  int port;
  BufferedReader br = null;
  Socket socket = null;

  // 该Map的键为STid,值为源元组的值
  private HashMap<String, String> waitAck = new HashMap<String, String>();

  SocketSpoutWithAck(String ip, String port) {
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

  /* 步骤2:接收网络元组,将元组绑定一个STid后发送到下游Bolt */
  @Override
  public void nextTuple() {
    try {
      String tuple;
      String STid = UUID.randomUUID().toString();
      if ((tuple = br.readLine()) != null) {
        waitAck.put(STid, tuple);
        collector.emit(new Values(tuple), STid);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /* 步骤3:声明输出元组的字段名称 */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sentence"));
  }

  /* 步骤4:描述元组ACK成功或失败后的处理逻辑 */
  @Override
  public void ack(Object STid) {
    // 当STid所对应的元组树中所有元组都得到成功处理时调用该方法
    // 根据STid删除waitAck中对应的元组
    waitAck.remove(STid);
  }

  @Override
  public void fail(Object STid) {
    // 当STid所对应的元组树中所有元组未得到成功处理时调用该方法
    // 重新发送waitAck中STid对应的元组
    collector.emit(new Values(waitAck.get(STid)), STid);
  }
}

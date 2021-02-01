package cn.edu.ecnu.mapreduce.examples.join.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class ReduceJoinWritable implements Writable {

  // 保存雇员表或部门表元组
  private String data;
  // 标识当前对象保存的元组来自雇员表还是部门表
  private String tag;

  // 用于标识的常量
  public static final String EMPLOYEE = "1";
  public static final String DEPARTMENT = "2";

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(tag);
    dataOutput.writeUTF(data);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    tag = dataInput.readUTF();
    data = dataInput.readUTF();
  }

  // get和set方法
  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }
}

package cn.edu.ecnu.mapreduce.examples.join.mapjoin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 步骤1：确定输入键值对[K1,V1]的数据类型为[LongWritable,Text]，确定输出键值对[K2,V2]的数据类型为[Text,NullWritable] */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

  private Map<String, String> departmentsTable = new HashMap<>();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    /* |步骤2：编写处理逻辑将[K1,V1]转换为[K2,V2]并输出| */
    if (departmentsTable.isEmpty()) {
      URI uri = context.getCacheFiles()[0];
      FileSystem fs = FileSystem.get(uri, new Configuration());
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
      String content;
      // 载入部门表
      while ((content = reader.readLine()) != null) {
        // 以制表符为分隔符对部门表元组进行切分，以便获取DeptName属性值
        String[] datas = content.split("\t");
        // 以DeptName属性值为key将元组保存在集合中
        departmentsTable.put(datas[0], datas[1]);
      }
    }
    // 以制表符为分隔符切分雇员表元组
    String[] datas = value.toString().split("\t");
    // 获取DeptName的属性值
    String deptName = datas[2];
    // 进行连接操作并输出
    if (departmentsTable.containsKey(deptName)) {
      context.write(
          new Text(value.toString() + "\t" + departmentsTable.get(deptName)), NullWritable.get());
    }
  }
}

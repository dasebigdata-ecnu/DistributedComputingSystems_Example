package cn.edu.ecnu.mapreduce.examples.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/* 步骤1：确定输入键值对[K2,List(V2)]的数据类型为[Text, IntWritable]，输出键值对[K3,V3]的数据类型为[Text,IntWritable] */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    /* 步骤2：编写处理逻辑将[K2,List(V2)]转换为[K3,V3]并输出 */
    int sum = 0;
    // 遍历累加求和
    for (IntWritable value : values) {
      sum += value.get();
    }
    // 输出计数结果
    context.write(key, new IntWritable(sum));
  }
}

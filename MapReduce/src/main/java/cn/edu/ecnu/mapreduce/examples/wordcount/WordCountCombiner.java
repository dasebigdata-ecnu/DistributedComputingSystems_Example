package cn.edu.ecnu.mapreduce.examples.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* 步骤1：确定输入键值对[K2,List(V2)]的数据类型为[Text, IntWritable]，输出键值对[K3,V3]的数据类型为[Text,IntWritable] */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    /* 步骤2：将[K2,List(V2)]合并为[K3,V3]并输出 */
    int sum = 0;
    // 进行合并操作
    for (IntWritable value : values) {
      sum += value.get();
    }
    // 输出合并的结果
    context.write(key, new IntWritable(sum));
  }
}

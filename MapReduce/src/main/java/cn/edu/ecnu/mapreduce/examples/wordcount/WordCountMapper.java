package cn.edu.ecnu.mapreduce.examples.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/* 步骤1：确定输入键值对[K1,V1]的数据类型为[LongWritable,Text]，输出键值对[K2,V2]的数据类型为[Text,IntWritable] */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    /* 步骤2：编写处理逻辑将[K1,V1]转换为[K2,V2]并输出 */
    // 以空格作为分隔符拆分成单词
    String[] datas = value.toString().split(" ");
    for (String data : datas) {
      // 输出分词结果
      context.write(new Text(data), new IntWritable(1));
    }
  }
}

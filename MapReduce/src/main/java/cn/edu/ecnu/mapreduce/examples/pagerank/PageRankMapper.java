package cn.edu.ecnu.mapreduce.examples.pagerank;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 步骤1：确定输出键值对[K1,V1]的数据类型为[LongWritable,Text]，确定输出键值对[K2,V2]的数据类型为[Text,ReducePageRankWritable] */
public class PageRankMapper extends Mapper<LongWritable, Text, Text, ReducePageRankWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    /* 步骤2：编写处理逻辑将[K1,V1]转换为[K2,V2]并输出 */
    // 以空格为分隔符切分
    String[] pageInfo = value.toString().split(" ");
    // 网页的排名值
    double pageRank = Double.parseDouble(pageInfo[1]);
    // 网页的出站链接数
    int outLink = (pageInfo.length - 2) / 2;
    ReducePageRankWritable writable;
    writable = new ReducePageRankWritable();
    // 计算贡献值并保存
    writable.setData(String.valueOf(pageRank / outLink));
    // 设置对应标识
    writable.setTag(ReducePageRankWritable.PR_L);
    // 对于每一个出站链接，输出贡献值
    for (int i = 2; i < pageInfo.length; i += 2) {
      context.write(new Text(pageInfo[i]), writable);
    }
    writable = new ReducePageRankWritable();
    // 保存网页信息并标识
    writable.setData(value.toString());
    writable.setTag(ReducePageRankWritable.PAGE_INFO);
    // 以输入的网页信息的网页名称为key进行输出
    context.write(new Text(pageInfo[0]), writable);
  }
}

package cn.edu.ecnu.mapreduce.examples.pagerank;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* 步骤1：确定输出键值对[K2,V2]的数据类型为[Text,ReducePageRankWritable]，确定输出键值对[K3,V3]的数据类型为[Text,NullWritable] */
public class PageRankReducer extends Reducer<Text, ReducePageRankWritable, Text, NullWritable> {

  // 阻尼系数
  private static final double D = 0.85;

  @Override
  protected void reduce(Text key, Iterable<ReducePageRankWritable> values, Context context)
      throws IOException, InterruptedException {
    /* 步骤2：编写处理逻辑将[K2,V2]转换为[K3,V3]并输出 */
    String[] pageInfo = null;
    // 从配置项中读取网页的总数
    int totalPage = context.getConfiguration().getInt(PageRank.TOTAL_PAGE, 0);
    // 从配置项中读取当前迭代步数
    int iteration = context.getConfiguration().getInt(PageRank.ITERATION, 0);
    double sum = 0;
    for (ReducePageRankWritable value : values) {
      String tag = value.getTag();
      // 如果是贡献值则进行求和，否则以空格为分隔符切分后保存到pageInfo
      if (tag.equals(ReducePageRankWritable.PR_L)) {
        sum += Double.parseDouble(value.getData());
      } else if (tag.equals(ReducePageRankWritable.PAGE_INFO)) {
        pageInfo = value.getData().split(" ");
      }
    }
    // 根据公式计算排名值
    double pageRank = (1 - D) / totalPage + D * sum;
    // 更新网页信息中的排名值
    pageInfo[1] = String.valueOf(pageRank);
    // 最后一次迭代输出网页名及排名值，而其余迭代输出网页信息
    StringBuilder result = new StringBuilder();
    if (iteration == (PageRank.MAX_ITERATION - 1)) {
      result.append(pageInfo[0]).append(" ").append(String.format("%.5f", pageRank));
    } else {
      for (String data : pageInfo) {
        result.append(data).append(" ");
      }
    }
    context.write(new Text(result.toString()), NullWritable.get());
  }
}

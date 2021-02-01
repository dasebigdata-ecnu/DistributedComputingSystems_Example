package cn.edu.ecnu.mapreduce.examples.pagerank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool {

  // 最大的迭代次数
  public static final int MAX_ITERATION = 20;
  // 从0开始记录当前迭代步数
  private static int iteration = 0;
  // 配置项中用于记录网页总数的键
  public static final String TOTAL_PAGE = "1";
  // 配置项中用于记录当前迭代步数的键
  public static final String ITERATION = "2";

  @Override
  public int run(String[] args) throws Exception {
    /* 步骤1：设置作业的信息 */
    int totalPage = Integer.parseInt(args[2]);
    getConf().setInt(PageRank.TOTAL_PAGE, totalPage);
    getConf().setInt(PageRank.ITERATION, iteration);

    Job job = Job.getInstance(getConf(), getClass().getSimpleName());
    // 设置程序的类名
    job.setJarByClass(getClass());

    // 设置数据的输入路径
    if (iteration == 0) {
      FileInputFormat.addInputPath(job, new Path(args[0]));
    } else {
      // 将上一次迭代的输出设置为输入
      FileInputFormat.addInputPath(job, new Path(args[1] + (iteration - 1)));
    }
    // 设置数据的输出路径
    FileOutputFormat.setOutputPath(job, new Path(args[1] + iteration));

    // 设置map方法及其输出键值对的数据类型
    job.setMapperClass(PageRankMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ReducePageRankWritable.class);

    // 设置reduce方法及其输出键值对的数据类型
    job.setReducerClass(PageRankReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    return job.waitForCompletion(true) ? 0 : -1;
  }

  public static void main(String[] args) throws Exception {
    /* 步骤2：运行作业 */
    int exitCode = 0;
    while (iteration < MAX_ITERATION) {
      exitCode = ToolRunner.run(new PageRank(), args);
      if (exitCode == -1) {
        break;
      }
      iteration++;
    }
    System.exit(exitCode);
  }
}

package cn.edu.ecnu.mapreduce.examples.join.mapjoin;

import java.net.URI;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapJoin extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    /* 步骤1：设置作业的信息 */
    Job job = Job.getInstance(getConf(), getClass().getSimpleName());
    // 设置程序的类名
    job.setJarByClass(getClass());

    // 设置数据的输入输出路径
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // 设置map方法及其输出键值对数据类型
    job.setMapperClass(MapJoinMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(0);

    // 将部门表通过分布式缓存广播出去
    job.addCacheFile(new URI(args[2]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    /* 步骤2：运行作业 */
    int exitCode = ToolRunner.run(new MapJoin(), args);
    System.exit(exitCode);
  }
}

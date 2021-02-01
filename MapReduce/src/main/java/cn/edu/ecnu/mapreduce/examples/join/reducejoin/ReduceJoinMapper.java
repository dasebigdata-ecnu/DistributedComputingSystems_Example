package cn.edu.ecnu.mapreduce.examples.join.reducejoin;

import cn.edu.ecnu.mapreduce.examples.Constants;
import cn.edu.ecnu.mapreduce.examples.join.entity.ReduceJoinWritable;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/* 步骤1：确定输入键值对[K1,V1]的数据类型为[LongWritable,Text]，确定输出键值对[K2,V2]的数据类型为[Text,ReduceJoinWritable] */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, ReduceJoinWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    /* 步骤2：编写处理逻辑将[K1,V1]转换为[K2,V2]并输出 */
    // 获取输入键值对所属的Split
    FileSplit split = (FileSplit) context.getInputSplit();
    // 通过Split获取键值对所属的文件路径
    String path = split.getPath().toString();
    ReduceJoinWritable writable = new ReduceJoinWritable();
    // 用writable的data保存元组
    writable.setData(value.toString());
    // 以制表符为分隔符切分元组，方便获取DeptName属性值
    String[] datas = value.toString().split("\t");
    // 通过输入数据所属文件路径判断datas的表来源并进行分类处理
    if (path.contains(Constants.EMPLOYEE)) {
      // 标识data保存的元组来自雇员表
      writable.setTag(ReduceJoinWritable.EMPLOYEE);
      // 用DeptName属性值为键输出结果
      context.write(new Text(datas[2]), writable);
    } else if (path.contains(Constants.DEPARTMENT)) {
      // 标识data中保存的元组来自部门表
      writable.setTag(ReduceJoinWritable.DEPARTMENT);
      // 用DeptName属性值为键输出结果
      context.write(new Text(datas[0]), writable);
    }
  }
}

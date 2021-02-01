package cn.edu.ecnu.mapreduce.examples.join.reducejoin;

import cn.edu.ecnu.mapreduce.examples.join.entity.ReduceJoinWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* 步骤1：确定输入键值对[K2,List(V2)]的数据类型为[Text, ReduceJoinWritable]，确定输出键值对[K3,V3]的数据类型为[Text,NullWritable] */
public class ReduceJoinReducer extends Reducer<Text, ReduceJoinWritable, Text, NullWritable> {

  @Override
  protected void reduce(Text key, Iterable<ReduceJoinWritable> values, Context context)
      throws IOException, InterruptedException {
    /* 步骤2：编写处理逻辑将输入键值对[K2,List(V2)]转换为[K3,V3]并输出 */
    List<String> employees = new ArrayList<>();
    List<String> departments = new ArrayList<>();
    // 分离values集合中雇员表和部门表元组
    for (ReduceJoinWritable value : values) {
      // 获取ReduceJoinWritable对象的标识
      String tag = value.getTag();
      if (tag.equals(ReduceJoinWritable.EMPLOYEE)) {
        employees.add(value.getData());
      } else if (tag.equals(ReduceJoinWritable.DEPARTMENT)) {
        departments.add(value.getData());
      }
    }

    // 进行连接操作并输出连接结果
    for (String employee : employees) {
      for (String department : departments) {
        String[] datas = department.split("\t");
        // 不重复输出DeptName属性值
        String result = employee + "\t" + datas[1];
        context.write(new Text(result), NullWritable.get());
      }
    }
  }
}

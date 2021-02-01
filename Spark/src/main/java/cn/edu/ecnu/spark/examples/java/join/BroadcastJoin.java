package cn.edu.ecnu.spark.examples.java.join;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BroadcastJoin {
  public static void run(String[] args) {
    /* 步骤1：通过SparkConf设置配置信息，并创建SparkContext */
    SparkConf conf = new SparkConf();
    conf.setAppName("ShuffleJoin");
    conf.setMaster("local"); // 仅用于本地进行调试，如在集群中运行则删除本行
    JavaSparkContext sc = new JavaSparkContext(conf);

    /* 步骤2：按应用逻辑使用操作算子编写DAG，其中包括RDD的创建、转换和行动等 */
    // 读入部门表
    Map<String, String> departmentsMap =
        sc.textFile("src/main/resources/input/join/department.csv")
            // 按制表符解析为[DeptName, Manager]键值对后collectAsMap到Driver中
            .mapToPair(
                new PairFunction<String, String, String>() {
                  @Override
                  public Tuple2<String, String> call(String line) throws Exception {
                    String[] tokens = line.split("\t");
                    return new Tuple2<String, String>(tokens[0], tokens[1]);
                  }
                })
            .collectAsMap();

    // 广播部门表
    Broadcast<Map<String, String>> departmentsBroadCast = sc.broadcast(departmentsMap);

    // 读入雇员表
    JavaRDD<List<String>> employeesRDD =
        sc.textFile("src/main/resources/input/join/employee.csv")
            // 按制表符解析为(DeptName, Name, EmpId)元组
            .map(
                new Function<String, List<String>>() {
                  @Override
                  public List<String> call(String line) throws Exception {
                    String[] tokens = line.split("\t");
                    List<String> list = new ArrayList();
                    list.add(tokens[2]);
                    list.add(tokens[0]);
                    list.add(tokens[1]);
                    return list;
                  }
                });

    // 在map转换操作中对雇员表与广播的部门表做自然连接
    employeesRDD
        .map(
            new Function<List<String>, List<String>>() {
              @Override
              public List<String> call(List<String> r) throws Exception {
                // 获取广播变量部门表的值
                Map<String, String> departmentsBroadCastValue = departmentsBroadCast.value();
                if (departmentsBroadCastValue.containsKey(r.get(0))) {
                  // 获取departmentsBroadCastValue中对应key的value
                  String left = departmentsBroadCastValue.get(r.get(0));
                  // 返回连接结果(Name, EmpId, DeptName, Manager)
                  List<String> joinList = new ArrayList<>();
                  joinList.add(r.get(1));
                  joinList.add(r.get(2));
                  joinList.add(r.get(0));
                  joinList.add(left);
                  return joinList;
                } else {
                  return null;
                }
              }
            })
        // 过滤空值
        .filter(
            new Function<List<String>, Boolean>() {
              @Override
              public Boolean call(List<String> joinList) throws Exception {
                return joinList != null;
              }
            })
        .foreach(item -> System.out.println(item));

    /* |步骤3：关闭SparkContext| */
    sc.stop();
  }

  public static void main(String[] args) {
    run(args);
  }
}

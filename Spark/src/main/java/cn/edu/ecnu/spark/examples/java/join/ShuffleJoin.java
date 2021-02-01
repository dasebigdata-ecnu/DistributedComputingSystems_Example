package cn.edu.ecnu.spark.examples.java.join;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShuffleJoin {
  public static void run(String[] args) {
    /* 步骤1：通过SparkConf设置配置信息，并创建SparkContext */
    SparkConf conf = new SparkConf();
    conf.setAppName("ShuffleJoin");
    conf.setMaster("local"); // 仅用于本地进行调试，如在集群中运行则删除本行
    JavaSparkContext sc = new JavaSparkContext(conf);

    /* 步骤2：按应用逻辑使用操作算子编写DAG，其中包括RDD的创建、转换和行动等 */
    // 读入部门表
    JavaPairRDD<String, String> departmentsRDD =
        sc.textFile("src/main/resources/input/join/department.csv")
            .mapToPair(
                new PairFunction<String, String, String>() {
                  @Override
                  public Tuple2<String, String> call(String line) throws Exception {
                    // 按制表符分割文本行，将每行文本映射为[DeptName, Manager]键值对
                    String[] tokens = line.split("\t");
                    return new Tuple2<String, String>(tokens[0], tokens[1]);
                  }
                });

    // 读入雇员表
    JavaPairRDD<String, List<String>> employeesRDD =
        sc.textFile("src/main/resources/input/join/employee.csv")
            .mapToPair(
                new PairFunction<String, String, List<String>>() {
                  @Override
                  public Tuple2<String, List<String>> call(String line) throws Exception {
                    // 按制表符分割文本行，将每行文本映射为[DeptName, Name EmpId]键值对
                    String[] tokens = line.split("\t");
                    List list = new ArrayList();
                    for (int i = 0; i <= 1; i++) {
                      list.add(tokens[i]);
                    }
                    return new Tuple2<String, List<String>>(tokens[2], list);
                  }
                });

    // 用coGroup算子对雇员表和部门表按DeptName聚合
    employeesRDD
        .cogroup(departmentsRDD)
        // 对[DeptName, {{Name EmpId}, {Manager}}]进行连接操作
        .flatMap(
            new FlatMapFunction<
                Tuple2<String, Tuple2<Iterable<List<String>>, Iterable<String>>>, List<String>>() {
              @Override
              public Iterator<List<String>> call(
                  Tuple2<String, Tuple2<Iterable<List<String>>, Iterable<String>>> tuple)
                  throws Exception {
                // 返回连接结果(Name, EmpId, DeptName, Manager)
                List<List<String>> list = new ArrayList<>();
                for (List<String> t1 : tuple._2._1) {
                  for (String t2 : tuple._2._2) {
                    List<String> newList = new ArrayList<>();
                    newList.add(t1.get(0));
                    newList.add(t1.get(1));
                    newList.add(tuple._1);
                    newList.add(t2);
                    list.add(newList);
                  }
                }
                return list.iterator();
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

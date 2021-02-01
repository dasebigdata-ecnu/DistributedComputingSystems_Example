package cn.edu.ecnu.spark.examples.java.kmeans;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class KMeans {

  // 计算两个的点距离的平方
  public static Double distanceSquared(List<Integer> p1, List<Double> p2) {
    Double sum = 0.0;
    for (int i = 0; i < p1.size(); i++) {
      sum += Math.pow(p1.get(i).doubleValue() - p2.get(i), 2);
    }
    return sum;
  }

  // 计算两个点的和
  public static List<Integer> addPoints(List<Integer> p1, List<Integer> p2) {
    List<Integer> newPoint = new ArrayList<>();
    for (int i = 0; i < p1.size(); i++) {
      newPoint.add(p1.get(i) + p2.get(i));
    }
    return newPoint;
  }

  // 计算一群点中距离某个点最近的点的角标
  public static Integer closestPoint(List<Integer> p, List<List<Double>> kPoints) {
    Integer bestIndex = 0;
    Double closest = Double.MAX_VALUE;
    for (int i = 0; i < kPoints.size(); i++) {
      Double dist = distanceSquared(p, kPoints.get(i));
      if (dist < closest) {
        closest = dist;
        bestIndex = Integer.valueOf(i);
      }
    }
    return bestIndex;
  }

  public static void run(String[] args) {
    /* 步骤1：通过SparkConf设置配置信息，并创建SparkContext */
    SparkConf conf = new SparkConf();
    conf.setAppName("KMeans");
    conf.setMaster("local"); // 仅用于本地进行调试，如在集群中运行则删除本行
    JavaSparkContext sc = new JavaSparkContext(conf);

    /* 步骤2：按应用逻辑使用操作算子编写DAG，其中包括RDD的创建、转换和行动等 */
    int iterateNum = 20; // 指定迭代次数
    // 从数据源读入数据点
    JavaRDD<List<Integer>> points =
        sc.textFile("src/main/resources/input/kmeans/data.txt")
            // 解析每行数据，并转换为Integer类型
            .map(
                new Function<String, List<Integer>>() {
                  @Override
                  public List<Integer> call(String line) throws Exception {
                    String[] tokens = line.split("\t")[0].split(",");
                    List<Integer> point = new ArrayList();
                    for (String t : tokens) {
                      point.add(Integer.valueOf(t));
                    }
                    return point;
                  }
                })
            .cache(); // 持久化到内存

    // 获取设置的初始中心点
    List<List<Double>> tmpKPoints =
        sc.textFile("src/main/resources/input/kmeans/centers.txt")
            // 解析每行数据，并转换为Integer类型
            .map(
                new Function<String, List<Double>>() {
                  @Override
                  public List<Double> call(String line) throws Exception {
                    String[] tokens = line.split(",");
                    List<Double> point = new ArrayList();
                    for (String t : tokens) {
                      point.add(Double.valueOf(t));
                    }
                    return point;
                  }
                })
            .collect();
    List<List<Double>> kPoints = new ArrayList<>(tmpKPoints);

    // 执行iterateNum次迭代计算
    for (int iter = 1; iter <= iterateNum - 1; iter++) {
      JavaPairRDD<Integer, Tuple2<List<Integer>, Integer>> closest =
          points.mapToPair(
              new PairFunction<List<Integer>, Integer, Tuple2<List<Integer>, Integer>>() {
                @Override
                public Tuple2<Integer, Tuple2<List<Integer>, Integer>> call(List<Integer> p)
                    throws Exception {
                  // 计算距离最近的聚类中心
                  return new Tuple2<>(closestPoint(p, kPoints), new Tuple2<>(p, 1));
                }
              });

      // 按类别号标识聚合，并计算新的聚类中心
      List<List<Double>> newPoints =
          closest
              .reduceByKey(
                  new Function2<
                      Tuple2<List<Integer>, Integer>,
                      Tuple2<List<Integer>, Integer>,
                      Tuple2<List<Integer>, Integer>>() {
                    @Override
                    public Tuple2<List<Integer>, Integer> call(
                        Tuple2<List<Integer>, Integer> t1, Tuple2<List<Integer>, Integer> t2)
                        throws Exception {
                      // 计算两个点的和，并累加数据点个数
                      return new Tuple2<>(addPoints(t1._1, t2._1), t1._2 + t2._2);
                    }
                  })
              .map(
                  new Function<Tuple2<Integer, Tuple2<List<Integer>, Integer>>, List<Double>>() {
                    @Override
                    public List<Double> call(Tuple2<Integer, Tuple2<List<Integer>, Integer>> t)
                        throws Exception {
                      List<Double> newPoint = new ArrayList<>();
                      // 每个维度的和值除以数据点个数得到每个维度的均值
                      for (int i = 0; i < t._2._1.size(); i++) {
                        newPoint.add(t._2._1.get(i).doubleValue() / t._2._2);
                      }
                      return newPoint;
                    }
                  })
              .collect();

      // 将旧的聚类中心替换为新的聚类中心
      for (int i = 0; i < kPoints.size(); i++) {
        kPoints.set(i, newPoints.get(i));
      }

      // 如果是最后一次迭代，则输出聚类结果
      if (iter == iterateNum - 1) {
        closest.foreach(
            item -> {
              for (int i = 0; i < item._2._1.size() - 1; i++) {
                System.out.print(item._2._1.get(i) + ",");
              }
              System.out.print(item._2._1.get(item._2._1.size() - 1) + " ");
              System.out.println((item._1.doubleValue() + 1));
            });
      }
    }

    /* 步骤3：关闭SparkContext */
    sc.stop();
  }

  public static void main(String[] args) throws IOException {
    run(args);
  }
}

package cn.edu.ecnu.spark.examples.scala.kmeans

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.math.pow


object KMeans {
  // 计算两个的点距离的平方
  def distanceSquared(p1: Array[Int], p2: Array[Double]): Double = {
    var sum = 0.0
    for (i <- 0 until p1.size) {
      sum += pow(p1(i).toDouble - p2(i), 2)
    }
    sum
  }

  // 计算两个点的和
  def addPoints(p1: Array[Int], p2: Array[Int]): Array[Int] = {
    val newPoint = ArrayBuffer[Int]()
    for (i <- 0 until p1.size) {
      newPoint += p1(i) + p2(i)
    }
    newPoint.toArray
  }

  // 计算一群点中距离某个点最近的点的角标
  def closestPoint(p: Array[Int], kPoints: Array[Array[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    // 遍历聚类中心集，并计算与数据点的距离
    for (i <- kPoints.indices) {
      // 计算数据点与当前聚类中心的距离
      val dist = distanceSquared(p, kPoints(i))
      if (dist < closest) {
        closest = dist
        bestIndex = i
      }
    }
    bestIndex
  }

  def run(args: Array[String]): Unit = {
    /* 步骤1：通过SparkConf设置配置信息，并创建SparkContext */
    val conf = new SparkConf()
      .setAppName("KMeans")
      .setMaster("local") // 仅用于本地进行调试，如在集群中运行则删除本行
    val sc = new SparkContext(conf)

    /* 步骤2：按应用逻辑使用操作算子编写DAG，其中包括RDD的创建、转换和行动等 */
    val iterateNum = 20 // 指定迭代次数

    // 从数据源读入数据点
    val points = sc.textFile("src/main/resources/input/kmeans/data.txt")
      // 解析每行数据，并转换为Int类型，并持久化到内存
      .map(_.split("\t")(0).split(",").map(_.toInt))
      .cache()

    // 获取设置的初始中心点
    val kPoints = sc.textFile("src/main/resources/input/kmeans/centers.txt")
      // 解析每行数据，并转换为Int类型
      .map(_.split(",").map(_.toDouble))
      .collect()

    // 执行iterateNum次迭代计算
    for (iter <- 1 to iterateNum - 1) {
      val closest = points.map(p => {
        // 计算距离最近的聚类中心
        (closestPoint(p, kPoints), (p, 1))
      })

      // 按类别号标识聚合，并计算新的聚类中心
      val newPoints = closest.reduceByKey {
        (t1, t2) => {
          // 计算两个点的和，并累加数据点个数
          (addPoints(t1._1, t2._1), t1._2 + t2._2)
        }
      }.map {
        case (index, (point, n)) => {
          val newPoint = ArrayBuffer[Double]()
          for (i <- point.indices) {
            // 每个维度的和值除以数据点个数得到每个维度的均值
            newPoint += point(i).toDouble / n
          }
          newPoint.toArray
        }
      }.collect()

      // 将旧的聚类中心替换为新的聚类中心
      for (i <- kPoints.indices) {
        kPoints(i) = newPoints(i)
      }

      // 如果是最后一次迭代，则输出聚类结果
      if (iter == iterateNum - 1) {
        closest.foreach(item => {
          for (i <- 0 until item._2._1.size - 1) {
            print(item._2._1(i) + ",")
          }
          print(item._2._1(item._2._1.size - 1) + " ")
          println((item._1 + 1).toDouble)
        })
      }

    }

    /* 步骤3：关闭SparkContext */
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}

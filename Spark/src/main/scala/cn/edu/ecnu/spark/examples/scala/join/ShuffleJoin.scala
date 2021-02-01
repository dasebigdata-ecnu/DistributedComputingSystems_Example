package cn.edu.ecnu.spark.examples.scala.join

import org.apache.spark.{SparkConf, SparkContext}

object ShuffleJoin {

  def run(args: Array[String]): Unit = {
    /* 步骤1：通过SparkConf设置配置信息，并创建SparkContext */
    val conf = new SparkConf()
      .setAppName("ShuffleJoin")
      .setMaster("local") // 仅用于本地进行调试，如在集群中运行则删除本行
    val sc = new SparkContext(conf)

    /* 步骤2：按应用逻辑使用操作算子编写DAG，其中包括RDD的创建、转换和行动等 */
    // 读入部门表
    val departmentsRDD = sc.textFile("src/main/resources/input/join/department.csv")
      .map(line => {
        // 按制表符分割文本行，将每行文本映射为[DeptName, Manager]键值对
        val tokens = line.split("\t")
        (tokens(0), tokens(1))
      })

    // 读入雇员表
    val employeesRDD = sc.textFile("src/main/resources/input/join/employee.csv")
      .map(line => {
        val tokens = line.split("\t")
        // 按制表符分割文本行，将每行文本映射为[DeptName, Name EmpId]键值对
        (tokens(2), (tokens(0), tokens(1)))
      })

    // 用coGroup算子对雇员表和部门表按DeptName聚合
    employeesRDD.cogroup(departmentsRDD, 2)
      // 对[DeptName, {{Name EmpId}, {Manager}}]进行连接操作
      .flatMap( tuple =>
        // 返回连接结果(Name, EmpId, DeptName, Manager)
        for (v <- tuple._2._1.iterator; w <- tuple._2._2.iterator) yield (v._1, v._2, tuple._1, w)
      )
      .foreach(println)

    /* |步骤3：关闭SparkContext| */
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}
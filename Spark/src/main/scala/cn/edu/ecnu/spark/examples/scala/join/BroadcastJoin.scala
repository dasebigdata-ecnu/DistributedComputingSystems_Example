package cn.edu.ecnu.spark.examples.scala.join

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastJoin {

  def run(args: Array[String]): Unit = {
    /* 步骤1：通过SparkConf设置配置信息，并创建SparkContext */
    val conf = new SparkConf()
      .setAppName("BroadcastJoin")
      .setMaster("local") // 仅用于本地进行调试，如在集群中运行则删除本行
    val sc = new SparkContext(conf)

    /* 步骤2：按应用逻辑使用操作算子编写DAG，其中包括RDD的创建、转换和行动等 */
    // 读入部门表
    val departmentsMap = sc.textFile("src/main/resources/input/join/department.csv")
      // 按制表符解析为[DeptName, Manager]键值对后collectAsMap到Driver中
      .map(line => {
        val tokens = line.split("\t")
        (tokens(0), tokens(1))
      }).collectAsMap()

    // 广播部门表
    val departmentsBroadCast = sc.broadcast(departmentsMap)

    // 读入雇员表
    val employeesRDD = sc.textFile("src/main/resources/input/join/employee.csv")
      .map(line => {
        // 按制表符解析为(DeptName, Name, EmpId)元组
        val tokens = line.split("\t")
        (tokens(2), tokens(0), tokens(1))
      })

    // 在map转换操作中对雇员表与广播的部门表做自然连接
    employeesRDD.map(r => {
      // 获取广播变量部门表的值
      val departmentsBroadCastValue = departmentsBroadCast.value
      if (departmentsBroadCastValue.contains(r._1)) {
        // 获取departmentsBroadCastValue中对应key的value
        val left = departmentsBroadCastValue.get(r._1).get
        // 返回连接结果(Name, EmpId, DeptName, Manager)
        (r._2, r._3, r._1, left)
      } else {
        null
      }
    })
      .filter(_ != null) // 过滤空值
      .foreach(println)

    /* |步骤3：关闭SparkContext| */
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}

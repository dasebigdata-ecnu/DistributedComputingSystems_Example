package cn.edu.ecnu.spark.examples.scala.pagerank

import org.apache.spark.{SparkConf, SparkContext}

object PageRank {
  def run(args: Array[String]): Unit = {
    /* 步骤1：通过SparkConf设置配置信息，并创建SparkContext */
    val conf = new SparkConf()
      .setAppName("PageRank")
      .setMaster("local") // 仅用于本地进行调试，如在集群中运行则删除本行
    val sc = new SparkContext(conf)

    /* 步骤2：按应用逻辑使用操作算子编写DAG，其中包括RDD的创建、转换和行动等 */
    val iterateNum = 20 // 指定迭代次数
    val factor = 0.85 // 指定系数
    // 读取输入文本数据
    val text = sc.textFile("src/main/resources/input/pagerank/pagerank.txt")

    // 将文本数据转换成[网页, {链接列表}]键值对
    val links = text.map(line => {
      val tokens = line.split(" ")
      var list = List[String]()
      for (i <- 2 until tokens.size by 2) {
        list = list :+ tokens(i)
      }
      (tokens(0), list)
    }).cache() // 持久化到内存

    val N = args(0).toLong // 从输入中获取网页总数N

    // 初始化每个页面的排名值，得到[网页, 排名值]键值对
    var ranks = text.map(line => {
      val tokens = line.split(" ")
      (tokens(0), tokens(1).toDouble)
    })

    // 执行iterateNum次迭代计算
    for (iter <- 1 to iterateNum) {
      val contributions = links
        // 将links和ranks做join，得到[网页, {{链接列表}, 排名值}]
        .join(ranks)
        // 计算出每个网页对其每个链接网页的贡献值
        .flatMap {
          case (pageId, (links, rank)) =>
            // 网页排名值除以链接总数
            links.map(dest => (dest, rank / links.size))
        }

      ranks = contributions
        // 聚合对相同网页的贡献值，求和得到对每个网页的总贡献值
        .reduceByKey(_ + _)
        // 根据公式计算得到每个网页的新排名值
        .mapValues(v => (1 - factor) * 1.0 / N + factor * v)

    }

    // 对排名值保留5位小数，并打印最终网页排名结果
    ranks.foreach(t => println(t._1 + " " + t._2.formatted("%.5f")))

    /* 步骤3：关闭SparkContext */
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}
package cn.edu.ecnu.spark.examples.scala.checkpoint

import org.apache.spark.{SparkConf, SparkContext}

object Checkpoint {
  def run(args: Array[String]): Unit = {
    /* 步骤1：通过SparkConf设置配置信息，并创建SparkContext */
    val conf = new SparkConf()
      .setAppName("Checkpoint")
      .setMaster("local") // 仅用于本地进行调试，如在集群中运行则删除本行
    val sc = new SparkContext(conf)

    // 设置检查点路径
    sc.setCheckpointDir("hdfs://localhost:9000/sout/ck001")

    /* 步骤2：按应用逻辑使用操作算子编写DAG，其中包括RDD的创建、转换和行动等 */
    val iterateNum = 20 // 指定迭代次数
    val factor = 0.85 // 指定系数

    // 读取输入文本数据
    val text = sc.textFile("src/main/resources/input/pagerank/pagerank.txt")

    // 将文本数据转换成(pageId, List(link0, link1, link2...))
    val links = text.map(line => {
      val tokens = line.split(" ")
      var list = List[String]()
      for (i <- 2 until tokens.size by 2) {
        list = list :+ tokens(i)
      }
      (tokens(0), list)
    }).cache() // 持久化到内存

    val N = args(0).toLong // 从输入中获取网页总数N

    // 初始化每个页面的排名值(pageId, rank)
    var ranks = text.map(line => {
      val tokens = line.split(" ")
      (tokens(0), tokens(1).toDouble)
    })

    // 执行iterateNum次迭代计算
    for (iter <- 1 to iterateNum) {
      val contributions = links
        // 将links和ranks做join，得到(pageId, (List(link0, link1, link2...), rank))
        .join(ranks)
        // 计算出每个page对其每个link目标page的贡献值
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

      // 每隔5次迭代保存一次检查点
      if (iter % 5 == 0) {
        // 将要设置检查点的RDD缓存在内存中，避免写检查点时二次计算
        ranks.cache()
        // 调用checkpoint方法设置检查点
        ranks.checkpoint()
      }

      // 对排名值保留5位小数，并打印每轮迭代的网页排名值
      ranks.foreach(t => println(t._1 + " " + t._2.formatted("%.5f")))
    }

    /* 步骤3：关闭SparkContext */
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}


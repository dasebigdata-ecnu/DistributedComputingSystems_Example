# 《分布式计算系统》示例代码

徐辰 编著 《分布式计算系统》 2022年9月

## [第2章 Hadoop文件系统](HDFS)

1. [写文件](HDFS/src/main/java/cn/edu/ecnu/hdfs/examples/write)
2. [读文件](HDFS/src/main/java/cn/edu/ecnu/hdfs/examples/read)

## [第3章 批处理系统MapReduce](MapReduce)

1. [词频统计](MapReduce/src/main/java/cn/edu/ecnu/mapreduce/examples/wordcount)
2. [关系表自然连接及其优化](MapReduce/src/main/java/cn/edu/ecnu/mapreduce/examples/join)
3. [网页链接排名](MapReduce/src/main/java/cn/edu/ecnu/mapreduce/examples/pagerank)
4. [K均值聚类](MapReduce/src/main/java/cn/edu/ecnu/mapreduce/examples/kmeans)

## [第4章 批处理系统Spark](Spark)

1. [词频统计](Spark/src/main/scala/cn/edu/ecnu/spark/examples/scala/wordcount)
2. [关系表自然连接及其优化](Spark/src/main/scala/cn/edu/ecnu/spark/examples/scala/join)
3. [网页链接排名](Spark/src/main/scala/cn/edu/ecnu/spark/examples/scala/pagerank)
4. [K均值聚类](Spark/src/main/scala/cn/edu/ecnu/spark/examples/scala/kmeans)
5. [检查点](Spark/src/main/scala/cn/edu/ecnu/spark/examples/scala/checkpoint)

## [第7章 流计算系统Storm](Storm)

1. [词频统计](Storm/src/main/java/cn/edu/ecnu/example/storm/wordcount/withoutAck)
2. [支持容错的词频统计](Storm/src/main/java/cn/edu/ecnu/example/storm/wordcount/withAck)
3. [简化的窗口操作](Storm/src/main/java/cn/edu/ecnu/example/storm/wordcount/window)
4. [异常检测](Storm/src/main/java/cn/edu/ecnu/example/storm/detection)

## [第8章 流计算系统Spark Streaming](SparkStreaming)

1. [按批词频统计](SparkStreaming/src/main/scala/cn/edu/ecnu/sparkstreaming/examples/scala/wordcount/BatchWordCount.scala)
2. [全局词频统计](SparkStreaming/src/main/scala/cn/edu/ecnu/sparkstreaming/examples/scala/wordcount/GlobalWordCount.scala)
3. [窗口操作](SparkStreaming/src/main/scala/cn/edu/ecnu/sparkstreaming/examples/scala/window)
4. [异常检测](SparkStreaming/src/main/scala/cn/edu/ecnu/sparkstreaming/examples/scala/anomaly)

## [第10章 批流融合系统Flink](Flink)

1. [词频统计](Flink/src/main/scala/cn/edu/ecnu/flink/examples/scala/wordcount)
2. [斐波那契数列生成](Flink/src/main/scala/cn/edu/ecnu/flink/examples/scala/fibonacciexample)
3. [整数求和*](Flink/src/main/scala/cn/edu/ecnu/flink/examples/scala/integersum)
4. [支持容错的词频统计](Flink/src/main/scala/cn/edu/ecnu/flink/examples/scala/wordcountwithfaulttolerance)

## [第11章 图处理系统Giraph](Giraph)

1. [连通分量](Giraph/src/main/java/cn/edu/ecnu/giraph/examples/cc)
2. [单源最短路径](Giraph/src/main/java/cn/edu/ecnu/giraph/examples/sssp)
3. [网页链接排名](Giraph/src/main/java/cn/edu/ecnu/giraph/examples/pagerank)
4. [K均值聚类](Giraph/src/main/java/cn/edu/ecnu/giraph/examples/kmeans)

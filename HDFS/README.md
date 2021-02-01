#### 功能说明

工程包含 HDFS 的读写操作示例

#### 运行方式

首先需要启动 HDFS，启动 HDFS 的命令为 `$HADOOP_HOME/sbin/start-dfs.sh`。
1. 运行写程序

   修改运行配置，在 Program arguments 中依次填入 `hdfs://localhost:9000/ecnu/hdfs/example.txt` 和 `src/main/resources/example/example.txt`

2. 运行读程序

   修改运行配置，在 Program arguments 中填入`hdfs://localhost:9000/ecnu/hdfs/example.txt` 和 `src/main/resources/example/output.txt`


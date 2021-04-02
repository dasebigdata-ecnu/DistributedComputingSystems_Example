#### 运行方法

##### Reduce 端 Join 部分

修改运行配置，在 Program arguments  中填入 `src/main/resources/inputs/join/ src/main/resources/outputs/reduceJoin`

##### Map 端 Join 部分

修改运行配置，在 Program arguments  中填入 `src/main/resources/inputs/join/employee.csv src/main/resources/outputs/mapJoin src/main/resources/inputs/join/department.csv`

**Note：在src/main/resources/inputs/join/input_cluster下放置了一组输入数据。当希望在集群上运行join作业时，可以将这组输入数据作为join作业的输入。需要说明的是，此时需要将这组数据上传至集群的HDFS，然后才能提交join作业。**
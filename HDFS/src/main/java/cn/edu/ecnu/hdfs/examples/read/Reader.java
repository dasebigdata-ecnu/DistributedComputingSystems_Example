package cn.edu.ecnu.hdfs.examples.read;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Reader {

  public void read(String hdfsFilePath, String localFilePath) throws IOException {
    /* 步骤1：获取HDFS的文件系统对象 */
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(hdfsFilePath), conf);
    /* 步骤2：获取输入流hdfsInputStream */
    FSDataInputStream hdfsInputStream = fs.open(new Path(hdfsFilePath));
    /* 步骤3：利用输入流读取HDFS文件 */
    // 写入本地文件的输出流
    FileOutputStream localOutputStream = new FileOutputStream(localFilePath);
    // 将HDFS文件的输入流拷贝至本地文件的输出流
    IOUtils.copyBytes(hdfsInputStream, localOutputStream, 4096, true);
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: <hdfsFilePath> <localFilePath>");
      System.exit(-1);
    }

    Reader reader = new Reader();
    reader.read(args[0], args[1]);
  }
}

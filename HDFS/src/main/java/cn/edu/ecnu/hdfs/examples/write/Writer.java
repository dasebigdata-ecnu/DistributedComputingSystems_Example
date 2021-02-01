package cn.edu.ecnu.hdfs.examples.write;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Writer {

  public void write(String hdfsFilePath, String localFilePath) throws IOException {

    /* 步骤1：获取HDFS的文件系统对象 */
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(hdfsFilePath), conf);

    /* 步骤2：获取输出流hdfsOutputStream */
    FSDataOutputStream hdfsOutputStream = fs.create(new Path(hdfsFilePath));

    /* 步骤3：利用输出流写入HDFS文件 */
    // 读取本地文件的输入流
    FileInputStream localInputStream = new FileInputStream(localFilePath);
    // 将本地文件的输入流拷贝至HDFS文件的输出流
    IOUtils.copyBytes(localInputStream, hdfsOutputStream, 4096, true);
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println("Usage: <hdfsFilePath> <localFilePath>");
      System.exit(-1);
    }

    Writer writer = new Writer();
    writer.write(args[0], args[1]);
  }
}

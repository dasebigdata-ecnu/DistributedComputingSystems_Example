package cn.edu.ecnu.mapreduce.examples.kmeans.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IOUtils;

public class CentersOperation {

  /**
   * 从指定路径读取中心点数据
   *
   * @param centersPath 中心数据路径
   * @param isDirectory 标识路径是否为目录
   * @return 返回中心数据
   */
  public static List<List<Double>> getCenters(String centersPath, boolean isDirectory) {
    List<List<Double>> centers = new ArrayList<>();

    try {
      if (isDirectory) {
        List<String> paths = FileOperation.getPaths(centersPath);
        if (paths == null) {
          throw new Exception(centersPath + "centers directory is empty");
        }
        for (String path : paths) {
          centers.addAll(getCenters(path, false));
        }
        return centers;
      }

      InputStream inputStream = FileOperation.read(centersPath);
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
      String line;
      while ((line = reader.readLine()) != null) {
        String[] datas = line.split(",");
        // 用集合保存每个中心信息
        List<Double> center = new ArrayList<>();
        for (String data : datas) {
          center.add(Double.parseDouble(data));
        }
        centers.add(center);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    return centers;
  }

  /**
   * 比较新旧中心数据，如果相同返回 true
   *
   * @param centerPath 旧的中心数据路径。即初始时刻设定的中心数据路径
   * @param newCenterPath mapReduce 生成的新的中心数据
   */
  public static boolean compareCenters(String centerPath, String newCenterPath) {
    List<List<Double>> centers = getCenters(centerPath, false);
    List<List<Double>> newCenters = getCenters(newCenterPath, true);

    double distance = 0;
    for (int i = 0; i < centers.size(); i++) {
      for (int j = 1; j < centers.get(0).size(); j++) {
        // 计算两个中心之间的距离
        distance += Math.pow(centers.get(i).get(j) - newCenters.get(i).get(j), 2);
      }
    }

    if (distance == 0) {
      // 中心相同则删除 mapReduce 生成的中心数据，方便输出聚类结果
      FileOperation.deletePath(newCenterPath, true);
      return true;
    } else {
      // 中心数据不相同则要将新的中心数据复制到初始中心数据路径处
      List<String> paths = FileOperation.getPaths(newCenterPath);
      try {
        if (paths == null) {
          throw new Exception("centers directory is empty");
        }
        for (String path : paths) {
          InputStream inputStream = FileOperation.read(path);
          OutputStream outputStream = FileOperation.write(centerPath, true);
          IOUtils.copyBytes(inputStream, outputStream, 4096, true);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      FileOperation.deletePath(newCenterPath, true);
    }
    return false;
  }
}

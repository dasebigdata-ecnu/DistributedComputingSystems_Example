package cn.edu.ecnu.giraph.examples.kmeans.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class PointsOperation {
  public static List<String> getCenters(String centersPath) {
    List<String> centers = new ArrayList<>();
    InputStream inputStream = FileOperation.read(centersPath);
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    try {
      while ((line = reader.readLine()) != null) {
        centers.add(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return centers;
  }

  public static List<List<Double>> parse(String pointsStr) {
    pointsStr = pointsStr.replace("\u0000", "");
    String[] datas = pointsStr.split("\t");
    List<List<Double>> points = new ArrayList<>();
    for (String data : datas) {
      List<Double> point = new ArrayList<>();
      for (String dimension : data.split("[ ,]")) {
        point.add(Double.parseDouble(dimension));
      }
      points.add(point);
    }
    return points;
  }
}

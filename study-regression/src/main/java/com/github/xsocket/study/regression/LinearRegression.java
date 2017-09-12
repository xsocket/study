package com.github.xsocket.study.regression;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Created by MWQ on 17/7/25.
 */
public class LinearRegression {

  private static final String COMMENT_SIGN = "#";
  private static final Splitter SPLITTER = Splitter.on(" ").omitEmptyStrings().trimResults();

  String resource;
  double[][] x;
  double[] y;

  public LinearRegression(String res) {
    this.resource = res;
  }

  public double[] compute() throws IOException {
    parseData(LinearRegression.readData(resource));
    OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
    regression.newSampleData(y, x);
    return regression.estimateRegressionParameters();
  }

  private void parseData(List<String> lines) {
    List<double[]> xList = Lists.newArrayList();
    List<Double> yList = Lists.newArrayList();
    for(String line : lines) {
      if(line.startsWith(COMMENT_SIGN)) {
        continue;
      }
      List<String> params = SPLITTER.splitToList(line);
      int last = params.size() - 1;
      yList.add(Double.parseDouble(params.get(last)));
      double[] xi = new double[last];
      for(int i = 0; i < last; i++) {
        xi[i] = Double.parseDouble(params.get(i));
      }
      xList.add(xi);
    }

    this.x = new double[xList.size()][];
    for(int j = 0; j < xList.size(); j++) {
      x[j] = xList.get(j);
    }
    this.y = new double[yList.size()];
    for(int k = 0; k < yList.size(); k++) {
      y[k] = yList.get(k);
    }
  }

  public static List<String> readData(String resource) throws IOException {
    InputStream stream = LinearRegression.class.getResourceAsStream(resource);
    try {
      return IOUtils.readLines(stream, "UTF-8");
    } finally {
      IOUtils.closeQuietly(stream);
    }
  }
}

package com.github.xsocket.study.zookeeper;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

/**
 * Created by MWQ on 17/7/24.
 */
public class CommonsMath {

  public static void main(String[] args) {
    OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
    double[] y = new double[]{68371049, 63130556, 94099531, 89598818, 88561592, 164344679, 86915184, 94904517, 86322777};
    double[][] x = new double[9][];
    x[0] = new double[]{10931149, 69799320, 0};
    x[1] = new double[]{8622376, 68371049, 0};
    x[2] = new double[]{21636738, 63130556, 0.2453};
    x[3] = new double[]{10959192, 101597572, 0};
    x[4] = new double[]{14817562, 89598818, 0.0751};
    x[5] = new double[]{37992274, 88561592, 0.5050};
    x[6] = new double[]{11347562, 91149660, 0.1083};
    x[7] = new double[]{16275562, 86915184, 0.3169};
    x[8] = new double[]{12230716, 94904517, 0.0871};
    regression.newSampleData(y, x);
    double[] beta = regression.estimateRegressionParameters();
    for(double d : beta) {
      System.out.println(d);
    }
  }
}

package com.github.xsocket.study.regression;

import org.junit.Test;

import java.io.IOException;

/**
 * Created by MWQ on 17/7/25.
 */
public class LinearRegressionTest {

  @Test
  public void testFirstDayBox() throws IOException {
    LinearRegression reg = new LinearRegression("/first-day-box.txt");
    double[] beta = reg.compute();
    for(double d : beta) {
      System.out.println(d);
    }
  }

  @Test
  public void testNation2_4() throws IOException {
    LinearRegression reg = new LinearRegression("/nation2-4.txt");
    double[] beta = reg.compute();
    for(double d : beta) {
      System.out.println(d);
    }
  }

  @Test
  public void testNation1() throws IOException {
    LinearRegression reg = new LinearRegression("/nation1.txt");
    double[] beta = reg.compute();
    for(double d : beta) {
      System.out.println(d);
    }
  }

  @Test
  public void testNation5() throws IOException {
    LinearRegression reg = new LinearRegression("/nation5.txt");
    double[] beta = reg.compute();
    for(double d : beta) {
      System.out.println(d);
    }
  }
}

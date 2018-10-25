/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.metrics;

import static com.memverge.mvfs.TestGroups.UT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = UT)
public class DMOMetricsMgrTest {

  private DMOMetricsMgr metricsManager = DMOMetricsMgr.getInstance();

  @BeforeMethod
  private void beforeMethod() {
    afterMethod();
    generateRandomMetrics();
  }

  @AfterMethod
  private void afterMethod() {
    metricsManager.reset();
  }

  public void testGetMetricsString() {
    generateRandomMetrics();

    val metricsTable = metricsManager.getMetricsString();
    log.info(metricsTable);
    val lines = metricsTable.split("\\n");
    val sepLen = lines[1].length();
    for (val line : lines) {
      assertThat(line.length()).isLessThanOrEqualTo(sepLen);
    }
    assertThat(lines.length).isEqualTo(Metric.values().length + 2);
  }

  private void generateRandomMetrics() {
    val rand = new Random(15L);
    Arrays.stream(Metric.values()).forEach(metric ->
        IntStream.range(0, 5).forEach(i -> {
          val took = Math.abs(rand.nextInt()) % 1000000;
          int randLong = rand.nextInt();
          val rc = randLong >= 0 ? randLong : -1;
          metric.add(took, rc);
        }));
  }
}

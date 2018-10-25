/*
 * Copyright (c) 2018 MemVerge Inc.
 */
package com.memverge.mvfs.metrics;

import static com.memverge.mvfs.TestGroups.UT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.IntStream;
import lombok.val;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = UT)
public class MetricTest {

  private Metric read = Metric.Read;

  @BeforeMethod
  private void beforeMethod() {
    read.reset();
  }

  public void testCount() {
    IntStream.range(0, 5).forEach(i -> read.add(123, 0));
    assertThat(read.count()).isEqualTo(5);
  }

  public void testTotalTime() {
    val took = new Double(1e9).longValue();
    IntStream.range(0, 5).forEach(i -> read.add(took, 0));
    assertThat(read.totalTime().getSeconds()).isEqualTo(5);
  }

  public void testOpsPerSecond() {
    val took = new Double(1e10).longValue();
    IntStream.range(0, 5).forEach(i -> read.add(took, 0));
    assertThat(read.opsPerSecond()).isEqualTo(0.1);
  }

  public void testBytesPerSecond() {
    val took = new Double(1e10).longValue();
    IntStream.range(0, 5).forEach(i -> read.add(took, 2 * 1024 * 1024));
    assertThat(read.mbPerSecond()).isEqualTo(0.2);
  }

  public void testIoMB() {
    val took = new Double(1e8).longValue();
    IntStream.range(0, 5).forEach(i -> read.add(took, 2 * 1024 * 1024));
    assertThat(read.ioMB()).isEqualTo(10.0);
  }

  public void testBytesPerSecondInvalid() {
    val took = new Double(1e10).longValue();
    IntStream.range(0, 5).forEach(i -> read.add(took, 0));
    assertThat(read.mbPerSecond()).isEqualTo(0.0);
  }

  public void testFailCount() {
    val took = new Double(1e8).longValue();
    IntStream.range(0, 5).forEach(i -> read.add(took, -1));
    assertThat(read.failCount()).isEqualTo(5);
  }

  public void testFailRatio() {
    val took = new Double(1e8).longValue();
    IntStream.range(0, 5).forEach(i -> read.add(took, -1));
    assertThat(read.failRatio()).isEqualTo(100.0);
  }

  public void testFailSet() {
    val took = new Double(1e8).longValue();
    IntStream.range(0, 5).forEach(i -> read.add(took, -i));
    assertThat(read.failRcSet()).containsExactly(-1L, -2L, -3L, -4L);
  }

  public void testToString() {
    val took = new Double(1e8).longValue();
    IntStream.range(0, 5).forEach(i -> read.add(took, -i));
    assertThat(read.toString()).contains("name=Read");
  }
}

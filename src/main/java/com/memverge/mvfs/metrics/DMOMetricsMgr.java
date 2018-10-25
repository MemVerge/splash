/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.metrics;

import static java.util.stream.Collectors.joining;

import com.memverge.mvfs.connection.MVFSConnectionMgr;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.ivy.util.StringUtils;

@Slf4j
public class DMOMetricsMgr {

  private boolean enabled = log.isDebugEnabled();

  private static class MetricsManagerHolder {

    private static final DMOMetricsMgr INSTANCE = new DMOMetricsMgr();
  }

  public static DMOMetricsMgr getInstance() {
    return MetricsManagerHolder.INSTANCE;
  }

  private DMOMetricsMgr() {
    reset();
  }

  public void reset() {
    Metric.resetAll();
  }

  public String getMetricsString() {
    val header = "name           count  fail   time(s)      op(us)     "
        + "   MB/s    IO(mb)  codes\n";
    val sb = new StringBuffer(header);
    sb.append(StringUtils.repeat("-", header.length()));
    Arrays.stream(Metric.values()).forEach(metric ->
        sb.append(String.format("\n%-10s%10d%6d%10.2f%12.2f%12.2f%10.2f  %s",
            metric.name(),
            metric.count(),
            metric.failCount(),
            metric.totalTime().toMillis() / 1000.0,
            metric.perOpLatencyUs(),
            metric.mbPerSecond(),
            metric.ioMB(),
            metric.failRcSet().stream().map(String::valueOf).collect(joining(",")))));
    return sb.toString();
  }

  public boolean isEnabled() {
    return enabled;
  }

  public DMOMetricsMgr setEnabled(boolean enabled) {
    this.enabled = enabled;
    MVFSConnectionMgr.cleanup();
    if (!enabled) {
      reset();
    }
    return this;
  }
}

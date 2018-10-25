/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.metrics;

import java.time.Duration;
import java.util.Set;
import lombok.val;
import org.apache.commons.lang3.builder.ToStringBuilder;

public enum Metric {
  Create,
  Delete,
  Open,
  Close,
  Exists,
  Mkdir,
  Rmdir,
  Rename,
  GetAttr,
  SetAttr,
  List,
  Connect,
  Disconnect,
  Broadcast,
  Prefetch,
  Read,
  Write,
  Mmap;

  private Counter counter;

  public static void resetAll() {
    for (val metric : Metric.values()) {
      metric.reset();
    }
  }

  Metric() {
    this.counter = new Counter();
  }

  public void reset() {
    counter.reset();
  }

  public void add(long durationInNanoseconds, long rc) {
    counter.add(durationInNanoseconds, rc);
  }

  public long count() {
    return counter.count();
  }

  public Duration totalTime() {
    return counter.totalTime();
  }

  public double opsPerSecond() {
    return counter.opsPerSecond();
  }

  public double perOpLatencyUs() {
    return counter.perOpLatencyUs();
  }

  public double mbPerSecond() {
    return counter.bytesPerSecond() / 1024D / 1024D;
  }

  public double failRatio() {
    return counter.failRatio();
  }

  public long failCount() {
    return counter.failCount();
  }

  public Set<Long> failRcSet() {
    return counter.getFailRcSet();
  }

  public double ioMB() {
    return counter.ioBytes() / 1024D / 1024D;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("name", name())
        .append("count", count())
        .append("total time(s)", totalTime().toMillis() / 1000D)
        .append("latency(us)", perOpLatencyUs())
        .append("ops/s", opsPerSecond())
        .append("MB/s", mbPerSecond())
        .append("IO(MB)", ioMB())
        .append("fail count", failCount())
        .append("fail ratio(%)", failRatio())
        .append("fail codes", failRcSet())
        .toString();
  }
}

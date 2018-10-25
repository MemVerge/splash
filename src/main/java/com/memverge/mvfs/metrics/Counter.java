/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.metrics;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.Value;
import lombok.val;

/**
 * Counter class used by jmvfs metrics.  It is responsible for all the
 * calculation and tracks the data.
 */
class Counter {

  private AtomicLong count = new AtomicLong();
  private AtomicLong total = new AtomicLong();
  private AtomicLong bytes = new AtomicLong();
  private AtomicLong failCount = new AtomicLong();
  @Getter
  private Set<Long> failRcSet = new HashSet<>();

  public void reset() {
    count.set(0);
    failCount.set(0);
    total.set(0);
    bytes.set(0);
    failRcSet.clear();
  }

  private void add(long durationInNanoseconds) {
    count.incrementAndGet();
    total.addAndGet(durationInNanoseconds);
  }

  public long count() {
    return count.get();
  }

  Duration totalTime() {
    return Duration.ofNanos(total.get());
  }

  double opsPerSecond() {
    return new OpsThroughput(totalTime(), count.get()).opsPerSecond();
  }

  double perOpLatencyUs() {
    return new OpsThroughput(totalTime(), count.get()).perOpLatencyUs();
  }

  double failRatio() {
    val fails = (double) failCount.get();
    val total = count.get();
    return fails / total * 100;
  }

  public void add(long durationInNanoseconds, long rc) {
    add(durationInNanoseconds);
    if (rc > 0) {
      this.bytes.addAndGet(rc);
    } else if (rc < 0) {
      failCount.incrementAndGet();
      failRcSet.add(rc);
    }
  }

  long failCount() {
    return failCount.get();
  }

  double bytesPerSecond() {
    return new DataThroughput(totalTime(), bytes.get()).bytesPerSecond();
  }

  long ioBytes() {
    return bytes.get();
  }
}

@Value
class DataThroughput {

  private Duration time;
  private long dataInBytes;

  double bytesPerSecond() {
    return dataInBytes * 1e9 / time.toNanos();
  }
}

@Value
class OpsThroughput {

  private Duration time;
  private long ops;

  double opsPerSecond() {
    return ops * 1e9 / time.toNanos();
  }

  double perOpLatencyUs() {
    return ((double) time.toNanos()) / ops / 1e3;
  }
}

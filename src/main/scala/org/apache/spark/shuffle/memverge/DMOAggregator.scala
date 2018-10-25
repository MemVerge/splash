/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package org.apache.spark.shuffle.memverge

import org.apache.spark.{Aggregator, TaskContext}

class DMOAggregator[K, V, C](
    agg: Aggregator[K, V, C])
    extends Aggregator[K, V, C](
      agg.createCombiner, agg.mergeValue, agg.mergeCombiners) {

  override def combineValuesByKey(
      iter: Iterator[_ <: Product2[K, V]],
      context: TaskContext): Iterator[(K, C)] = {
    val combiners = new DMOAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)
    combiners.insertAll(iter)
    updateMetrics(context, combiners)
    combiners.iterator
  }

  override def combineCombinersByKey(
      iter: Iterator[_ <: Product2[K, C]],
      context: TaskContext): Iterator[(K, C)] = {
    val combiners = new DMOAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
    combiners.insertAll(iter)
    updateMetrics(context, combiners)
    combiners.iterator
  }

  /** Update task metrics after populating the external map. */
  private def updateMetrics(context: TaskContext, map: DMOAppendOnlyMap[_, _, _]): Unit = {
    Option(context).foreach { c =>
      c.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
      c.taskMetrics().incDiskBytesSpilled(map.dmoBytesSpilled)
      c.taskMetrics().incPeakExecutionMemory(map.peakMemoryUsedBytes)
    }
  }
}

/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle.memverge

import java.io.IOException

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReader}
import org.apache.spark.storage.BlockId
import org.apache.spark.util.CompletionIterator
import org.apache.spark.{InterruptibleIterator, MapOutputTracker, SparkEnv, TaskContext}

/**
 * Fetches and reads the partitions in range [startPartition, endPartition)
 * from a shuffle by requesting them from DMO.
 */
private[spark] class DMOShuffleReader[K, C](
    resolver: DMOShuffleBlockResolver,
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
    extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  /** @inheritdoc */
  override def read(): Iterator[Product2[K, C]] = {
    val shuffleBlocks = mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId, startPartition, endPartition)
        .flatMap(_._2)
    readShuffleBlocks(shuffleBlocks)
  }

  def readShuffleBlocks(shuffleBlocks: Seq[(BlockId, Long)]): Iterator[Product2[K, C]] = {
    val fetcherIterator = new DMOShuffleFetcherIterator(resolver, shuffleBlocks)

    val readMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    val serializer = DMOSerializer(dep)

    val recordIter = fetcherIterator.flatMap { case (blockId, stream, length) =>
      try {
        readMetrics.incLocalBlocksFetched(1)
        readMetrics.incLocalBytesRead(length)
        serializer.deserializeStream(blockId, stream).asKeyValueIterator
      } catch {
        case ioe: IOException =>
          logError(s"Failed to read ${blockId.name}")
          throw ioe
      }
    }

    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      val aggregator = dep.aggregator.map(new DMOAggregator(_)).get
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        aggregator.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        aggregator.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data.
        val sorter =
          new DMOSorter[K, C, C](context, ordering = Some(keyOrd), serializer = DMOSerializer(dep))
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.dmoBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }
  }
}

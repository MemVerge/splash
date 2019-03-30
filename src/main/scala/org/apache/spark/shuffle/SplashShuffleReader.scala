/*
 * Modifications copyright (C) 2018 MemVerge Inc.
 *
 * Replace the original shuffle class with Splash version classes.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.shuffle

import java.io.IOException

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.util.CompletionIterator
import org.apache.spark.{InterruptibleIterator, MapOutputTracker, SparkEnv, TaskContext}

/**
 * Fetches and reads the partitions in range [startPartition, endPartition)
 * from a shuffle by requesting them from storage.
 */
private[spark] class SplashShuffleReader[K, C](
    resolver: SplashShuffleBlockResolver,
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
    extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  /** @inheritdoc*/
  override def read(): Iterator[Product2[K, C]] = {
    val shuffleBlocks = mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId, startPartition, endPartition)
        .flatMap(_._2)
    readShuffleBlocks(shuffleBlocks)
  }

  def readShuffleBlocks(shuffleBlocks: Iterator[(BlockId, Long)]): Iterator[Product2[K, C]] = {
    val fetcherIterator = new SplashShuffleFetcherIterator(resolver, shuffleBlocks)

    val readMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    val serializer = SplashSerializer(dep)

    val recordIter = fetcherIterator.flatMap { case (blockId, stream, length) =>
      try {
        readMetrics.incLocalBlocksFetched(1)
        readMetrics.incLocalBytesRead(length)
        serializer.deserializeStream(blockId, stream).asKeyValueIterator
      } catch {
        case ioe: IOException =>
          logError(s"Failed to read ${blockId.name}")
          resolver.dump(blockId)
          throw ioe
      }
    }

    def dumpCurrentPartitionOnError[T](f: ()=>T): T = {
      try {
        f()
      } catch {
        case e: Exception =>
          log.error(s"dump current partition on error.", e)
          fetcherIterator.dump()
          throw e
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
      val aggregator = dep.aggregator.map(new SplashAggregator(_)).get
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        aggregator.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]

        dumpCurrentPartitionOnError(() => {
          aggregator.combineValuesByKey(keyValuesIterator, context)
        })
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
          new SplashSorter[K, C, C](context, ordering = Some(keyOrd), serializer = SplashSerializer(dep))

        dumpCurrentPartitionOnError(() => {
          sorter.insertAll(aggregatedIter)
        })

        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.bytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }
  }
}

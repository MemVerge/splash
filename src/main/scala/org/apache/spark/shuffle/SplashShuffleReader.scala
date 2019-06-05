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

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.{InterruptibleIterator, MapOutputTracker, SparkEnv, TaskContext}

/**
 * Fetches and reads the partitions in range [startPartition, endPartition)
 * from a shuffle by requesting them from storage.
 */
private[spark] class SplashShuffleReader[K, V](
    resolver: SplashShuffleBlockResolver,
    handle: BaseShuffleHandle[K, _, V],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
    extends ShuffleReader[K, V] with Logging {

  private val dep = handle.dependency

  private type Pair = (Any, Any)
  private type KCPair = (K, V)
  private type KCIterator = Iterator[KCPair]

  override def read(): KCIterator = {
    val shuffleBlocks = mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId, startPartition, endPartition)
        .flatMap(_._2)
    readShuffleBlocks(shuffleBlocks)
  }

  def readShuffleBlocks(shuffleBlocks: Seq[(BlockId, Long)]): KCIterator =
    readShuffleBlocks(shuffleBlocks.iterator)

  def readShuffleBlocks(shuffleBlocks: Iterator[(BlockId, Long)]): KCIterator = {
    val taskMetrics = context.taskMetrics()
    val serializer = SplashSerializer(dep)

    val nonEmptyBlocks = shuffleBlocks.filter(_._2 > 0).map(_._1)
    val fetcherIterator = SplashShuffleFetcherIterator(resolver, nonEmptyBlocks)

    def getAggregatedIterator(iterator: Iterator[Pair]): KCIterator = {
      dep.aggregator match {
        case Some(agg) =>
          val aggregator = new SplashAggregator(agg)
          if (dep.mapSideCombine) {
            // We are reading values that are already combined
            val combinedKeyValuesIterator = iterator.asInstanceOf[Iterator[(K, V)]]
            aggregator.combineCombinersByKey(combinedKeyValuesIterator, context)
          } else {
            // We don't know the value type, but also don't care -- the dependency *should*
            // have made sure its compatible w/ this aggregator, which will convert the value
            // type to the combined type C
            val keyValuesIterator = iterator.asInstanceOf[Iterator[(K, Nothing)]]
            aggregator.combineValuesByKey(keyValuesIterator, context)
          }
        case None =>
          require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
          iterator.asInstanceOf[KCIterator]
      }
    }

    def getSortedIterator(iterator: KCIterator): KCIterator = {
      // Sort the output if there is a sort ordering defined.
      dep.keyOrdering match {
        case Some(keyOrd: Ordering[K]) =>
          // Create an ExternalSorter to sort the data.
          val sorter = new SplashSorter[K, V, V](
            context, ordering = Some(keyOrd), serializer = serializer)
          sorter.insertAll(iterator)
          sorter.updateTaskMetrics()
          sorter.completionIterator
        case None =>
          iterator
      }
    }

    val metricIter = fetcherIterator.flatMap(
      _.asMetricIterator(serializer, taskMetrics))

    // An interruptible iterator must be used here in order to support task cancellation
    getSortedIterator(
      getAggregatedIterator(
        new InterruptibleIterator[Pair](context, metricIter)))
  }
}

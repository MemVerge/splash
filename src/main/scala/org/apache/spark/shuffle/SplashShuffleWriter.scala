/*
 * Modifications copyright (C) 2018 MemVerge Inc.
 *
 * Modified to use the Splash version classes.
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

import org.apache.spark.TaskContext
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.ShuffleBlockId

/**
 * Write shuffle file with the help of SplashSorter
 * Each mapper will write it's own output file partitioned by reducer.
 */
private[spark] class SplashShuffleWriter[K, V, C](
    resolver: SplashShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,
    context: TaskContext)
    extends ShuffleWriter[K, V] with Logging {

  private var sorter: SplashSorter[K, V, _] = _

  private var stopping = false

  private var mapStatus: MapStatus = _

  private var partitionLengths: Array[Long] = _

  private val writeMetrics = if (context != null) {
    context.taskMetrics().shuffleWriteMetrics
  } else {
    new ShuffleWriteMetrics()
  }

  private val dep = handle.dependency

  /**
   * @inheritdoc
   *
   * Write shuffle to storage.
   */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val serializer = SplashSerializer(dep)
    sorter = if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      new SplashSorter[K, V, C](
        context,
        dep.aggregator,
        Some(dep.partitioner),
        dep.keyOrdering,
        serializer)
    } else {
      new SplashSorter[K, V, V](
        context,
        aggregator = None,
        Some(dep.partitioner),
        ordering = None,
        serializer)
    }
    sorter.insertAll(records)

    val tmp = resolver.getDataTmpFile(dep.shuffleId, mapId)
    try {
      val start = System.nanoTime()
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, resolver.NOOP_REDUCE_ID)
      partitionLengths = sorter.writePartitionedFile(blockId, tmp)
      resolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
      mapStatus = MapStatus(resolver.blockManagerId, partitionLengths)
      val milliSeconds = (System.nanoTime() - start) / 1e6
      logDebug(s"mapper $mapId wrote ${partitionLengths.sum} bytes to " +
          s"${blockId.name} in $milliSeconds milli-seconds.")
    } finally {
      tmp.recall()
    }
  }

  private[spark] def getPartitionLengths = partitionLengths

  /** @inheritdoc */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        None
      } else {
        stopping = true
        if (success) {
          Option(mapStatus)
        } else {
          None
        }
      }
    } finally {
      if (sorter != null) {
        val startTime = System.nanoTime
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        sorter = null
      }
    }
  }
}

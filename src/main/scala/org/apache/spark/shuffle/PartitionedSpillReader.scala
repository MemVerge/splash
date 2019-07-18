/*
 * Copyright (C) 2018 MemVerge Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.shuffle

import org.apache.spark.internal.Logging

private class PartitionedSpillReader[K, V](
    spill: SpilledFile, serializer: SplashSerializer)
    extends PairSpillReader(spill, serializer) with Logging {

  private val numPartitions = spill.elementsPerPartition.length

  private var partitionId = 0
  private var indexInPartition = 0L
  private var lastPartitionId = 0

  skipToNextPartition()

  private def skipToNextPartition(): Unit = {
    while (partitionId < numPartitions &&
        indexInPartition == spill.elementsPerPartition(partitionId)) {
      partitionId += 1
      indexInPartition = 0L
    }
  }

  private def finished = partitionId == numPartitions

  override protected def readNextItem(): KVPair = {
    if (finished || keyValueIterator == null) {
      null
    } else {
      val pair = keyValueIterator.next()
      lastPartitionId = partitionId
      if (!keyValueIterator.hasNext) {
        keyValueIterator = nextBatchIterator()
      }
      indexInPartition += 1
      skipToNextPartition()
      pair
    }
  }

  private var nextPartitionToRead = 0

  def readNextPartition(): KVIterator = {
    val reader = this
    new KVIterator {
      private val myPartition = nextPartitionToRead
      nextPartitionToRead += 1

      override def hasNext: Boolean = {
        if (!reader.hasNext) return false
        assert(lastPartitionId >= myPartition)
        lastPartitionId == myPartition
      }

      override def next(): KVPair = reader.next()
    }
  }

  def iterator(): Iterator[((Int, K), V)] =
    map(kv => ((lastPartitionId, kv._1), kv._2))
}

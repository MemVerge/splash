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

import java.io.InputStream

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.DeserializationStream

private class PairSpillReader[K, V](
    spill: SpilledFile,
    serializer: SplashSerializer)
    extends Iterator[(K, V)] with Logging {
  protected type KVPair = (K, V)
  protected type KVIterator = Iterator[KVPair]

  private var inputStream: InputStream = _
  private var nextItem: KVPair = _

  private val batchOffsets = spill.batchOffsets
  private var startBatch = 0
  private var endBatch = batchOffsets.length - 1

  protected var keyValueIterator: KVIterator = nextBatchIterator()

  protected def readNextItem(): KVPair = {
    if (keyValueIterator == null) {
      null
    } else {
      val pair = keyValueIterator.next()
      if (!keyValueIterator.hasNext) {
        keyValueIterator = nextBatchIterator()
      }
      pair
    }
  }

  private def nextBatchStream(): DeserializationStream = {
    if (startBatch < endBatch) {
      close()

      val start = batchOffsets(startBatch)
      val end = batchOffsets(startBatch + 1)
      startBatch += 1
      logDebug(s"read batch $startBatch offset $start - $end from spill")

      inputStream = spill.file.makeBufferedInputStreamWithin(start, end)
      serializer.deserializeStream(spill.blockId, inputStream)
    } else {
      cleanup()
      null
    }
  }

  def limit(startBatch: Int, endBatch: Int): PairSpillReader[K, V] = {
    this.startBatch = startBatch
    this.endBatch = endBatch
    try {
      this.keyValueIterator = nextBatchIterator()
    } catch {
      case _: ArrayIndexOutOfBoundsException =>
        // input batch index is not valid.
        logWarning(s"$startBatch or $endBatch should be smaller " +
            s"than ${batchOffsets.length}.  mark this iterator empty.")
        this.keyValueIterator = null
    }
    this
  }

  def nextBatchIterator(): KVIterator = {
    val stream = nextBatchStream()
    if (stream != null) {
      stream.asKeyValueIterator.asInstanceOf[KVIterator]
    } else {
      null
    }
  }

  def cleanup(): Unit = {
    startBatch = batchOffsets.length
    close()
  }

  def close(): Unit = {
    if (inputStream != null) {
      inputStream.close()
      inputStream = null
    }
  }

  override def hasNext: Boolean = {
    if (nextItem == null) {
      nextItem = readNextItem()
    }
    nextItem != null
  }

  override def next(): KVPair = {
    if (!hasNext) {
      throw new NoSuchElementException
    }
    val item = nextItem
    nextItem = null
    item
  }
}

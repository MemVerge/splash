/*
 * Modifications copyright (C) 2018 MemVerge Inc.
 *
 * Use general IO interface to replace the raw File class.
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

import java.io.EOFException

import com.memverge.splash.{StorageFactoryHolder, TmpShuffleFile}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{BlockManager, TempLocalBlockId}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.{SizeTracker, SizeTrackingAppendOnlyMap, Spillable}
import org.apache.spark.{SparkEnv, TaskContext}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{BufferedIterator, mutable}

/**
 * :: DeveloperApi ::
 * An append-only map that spills sorted content to storage when there is insufficient space for it
 * to grow.
 *
 * This map takes two passes over the data:
 *
 * (1) Values are merged into combiners, which are sorted and spilled to storage as necessary
 * (2) Combiners are read from disk and merged together
 *
 * The setting of the spill threshold faces the following trade-off: If the spill threshold is
 * too high, the in-memory map may occupy more memory than is available, resulting in OOM.
 * However, if the spill threshold is too low, we spill frequently and incur unnecessary storage
 * writes. This may lead to a performance regression compared to the normal case of using the
 * non-spilling AppendOnlyMap.
 */
private[spark] class SplashAppendOnlyMap[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    serializer: SplashSerializer = SplashSerializer.defaultSerializer(),
    context: TaskContext = TaskContext.get())
    extends Spillable[SizeTracker](context.taskMemoryManager())
    with Serializable
    with Logging
    with Iterable[(K, C)] {

  // Backwards-compatibility constructor for binary compatibility
  def this(
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      serializer: SplashSerializer,
      blockManager: BlockManager) {
    this(createCombiner, mergeValue, mergeCombiners, serializer, TaskContext.get())
  }

  @volatile private var currentMap = new SizeTrackingAppendOnlyMap[K, C]

  private val storageFactory = StorageFactoryHolder.getFactory

  private val spilledMaps = new ArrayBuffer[MapIterator]
  private val conf = SparkEnv.get.conf

  private var _bytesSpilled = 0L
  private var _peakMemoryUsedBytes: Long = 0L

  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  def bytesSpilled: Long = _bytesSpilled

  private val fileBufferSize = conf.get(SplashOpts.shuffleFileBufferKB).toInt * 1024

  private val keyComparator = new SplashHashComparator[K]
  private var isHugeValueBuffer = false

  @volatile private var readingIterator: SplashSpillableIterator[(K, C)] = _

  private[spark] def numSpills: Int = spilledMaps.size

  def insert(key: K, value: V): Unit = {
    insertAll(Iterator((key, value)))
  }

  def insertAll(entries: Iterator[Product2[K, V]]): Unit = {
    if (currentMap == null) {
      throw new IllegalStateException(
        "cannot insert new elements into a map after calling iterator.")
    }

    var curEntry: Product2[K, V] = null
    val update: (Boolean, C) => C = (hadVal, oldVal) =>
      if (hadVal) {
        mergeValue(oldVal, curEntry._2)
      } else {
        createCombiner(curEntry._2)
      }

    while (entries.hasNext) {
      curEntry = entries.next()
      val estimateSize = currentMap.estimateSize()
      if (estimateSize > _peakMemoryUsedBytes) {
        _peakMemoryUsedBytes = estimateSize
      }
      if (isHugeValueBuffer) {
        spill(currentMap)
        currentMap = new SizeTrackingAppendOnlyMap[K, C]
      } else if (maybeSpill(currentMap, estimateSize)) {
        currentMap = new SizeTrackingAppendOnlyMap[K, C]
      }
      val newValue = currentMap.changeValue(curEntry._1, update)
      addElementsRead()
      if (elementsRead % 10000 == 0) {
        newValue match {
          case s: Seq[_] =>
            isHugeValueBuffer = s.size > 10000000
            if (isHugeValueBuffer) {
              logInfo(s"map has huge value size ${s.size}, force spill")
            }
          case _ => // do nothing
        }
      }
    }
  }

  def insertAll(entries: Iterable[Product2[K, V]]): Unit = {
    insertAll(entries.iterator)
  }

  override protected[this] def spill(collection: SizeTracker): Unit = {
    val inMemoryIterator = currentMap.destructiveSortedIterator(keyComparator)
    val spillFile = spillInMemoryIterator(inMemoryIterator).file
    val mapIterator = new MapIterator(spillFile)
    spilledMaps += mapIterator
    isHugeValueBuffer = false
    SplashAppendOnlyMap.spilledCount += 1
  }

  override protected[this] def forceSpill(): Boolean = {
    if (readingIterator != null) {
      readingIterator.spill() match {
        case Some(_) => currentMap = null; true
        case None => false
      }
    } else if (currentMap.size > 0) {
      spill(currentMap)
      currentMap = new SizeTrackingAppendOnlyMap[K, C]
      true
    } else {
      false
    }
  }

  override def iterator: Iterator[(K, C)] = {
    if (currentMap == null) {
      throw new IllegalStateException(
        "iterator is destructive and should only be called once.")
    }
    if (spilledMaps.isEmpty) {
      CompletionIterator[(K, C), Iterator[(K, C)]](
        destructiveIterator(currentMap.iterator), freeCurrentMap())
    } else {
      new TupleIterator()
    }
  }

  private def freeCurrentMap(): Unit = {
    if (currentMap != null) {
      currentMap = null
      releaseMemory()
    }
  }

  def cleanupSpillFile(): Unit = {
    spilledMaps.foreach(_.cleanup())
  }

  private class TupleIterator extends Iterator[(K, C)] {
    private val mergeHeap = new mutable.PriorityQueue[StreamBuffer]

    private val sortedMap = CompletionIterator[(K, C), Iterator[(K, C)]](
      destructiveIterator(currentMap.destructiveSortedIterator(keyComparator)), freeCurrentMap())

    private val inputStreams = (Seq(sortedMap) ++ spilledMaps).map(it => it.buffered)

    inputStreams.foreach { it =>
      val kcPairs = new ArrayBuffer[(K, C)]
      readNextHashCode(it, kcPairs)
      if (kcPairs.nonEmpty) {
        mergeHeap.enqueue(new StreamBuffer(it, kcPairs))
      }
    }

    private def readNextHashCode(it: BufferedIterator[(K, C)], buf: ArrayBuffer[(K, C)]): Unit = {
      if (it.hasNext) {
        var kc = it.next()
        buf += kc
        val minHash = hashKey(kc)
        while (it.hasNext && it.head._1.hashCode() == minHash) {
          kc = it.next()
          buf += kc
        }
      }
    }

    /**
     * If the given buffer contains a value for the given key, merge that value into
     * baseCombiner and remove the corresponding (K, C) pair from the buffer.
     */
    private def mergeIfKeyExists(key: K, baseCombiner: C, buffer: StreamBuffer): C = {
      var i = 0
      while (i < buffer.pairs.length) {
        val pair = buffer.pairs(i)
        if (pair._1 == key) {
          removeFromBuffer(buffer.pairs, i)
          return mergeCombiners(baseCombiner, pair._2)
        }
        i += 1
      }
      baseCombiner
    }

    private def removeFromBuffer[T](buffer: ArrayBuffer[T], index: Int): T = {
      val elem = buffer(index)
      buffer(index) = buffer(buffer.size - 1)
      buffer.reduceToSize(buffer.size - 1)
      elem
    }

    override def hasNext: Boolean = mergeHeap.nonEmpty

    override def next(): (K, C) = {
      if (mergeHeap.isEmpty) {
        throw new NoSuchElementException
      }

      val minBuffer = mergeHeap.dequeue()
      val minPairs = minBuffer.pairs
      val minHash = minBuffer.minKeyHash
      val minPair = removeFromBuffer(minPairs, 0)
      val minKey = minPair._1
      var minCombiner = minPair._2

      val mergedBuffers = ArrayBuffer[StreamBuffer](minBuffer)
      while (mergeHeap.nonEmpty && mergeHeap.head.minKeyHash == minHash) {
        val newBuffer = mergeHeap.dequeue()
        minCombiner = mergeIfKeyExists(minKey, minCombiner, newBuffer)
        mergedBuffers += newBuffer
      }

      mergedBuffers.foreach { buffer =>
        if (buffer.isEmpty) {
          readNextHashCode(buffer.iterator, buffer.pairs)
        }
        if (!buffer.isEmpty) {
          mergeHeap.enqueue(buffer)
        }
      }

      (minKey, minCombiner)
    }
  }

  private class StreamBuffer(
      val iterator: BufferedIterator[(K, C)],
      val pairs: ArrayBuffer[(K, C)])
      extends Comparable[StreamBuffer] {

    def isEmpty: Boolean = pairs.isEmpty

    // Invalid if there are no more pairs in this stream
    def minKeyHash: Int = {
      assert(pairs.nonEmpty)
      hashKey(pairs.head)
    }

    override def compareTo(other: StreamBuffer): Int = {
      // descending order because mutable.PriorityQueue dequeues the max, not the min
      if (other.minKeyHash < minKeyHash) -1 else if (other.minKeyHash == minKeyHash) 0 else 1
    }
  }

  private class MapIterator(tmpFile: TmpShuffleFile) extends Iterator[(K, C)] {
    private var nextItem: (K, C) = _
    private var objectsRead = 0

    private val deserializeStream = serializer.deserializeStream(tmpFile)

    private def readNextItem(): (K, C) = {
      try {
        val k = deserializeStream.readKey().asInstanceOf[K]
        val c = deserializeStream.readValue().asInstanceOf[C]
        objectsRead += 1
        (k, c)
      } catch {
        case _: EOFException | _: NullPointerException =>
          cleanup()
          null
      }
    }

    override def hasNext: Boolean = {
      if (nextItem == null) {
        nextItem = readNextItem()
      }
      nextItem != null
    }

    override def next(): (K, C) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val item = nextItem
      nextItem = null
      item
    }

    def cleanup(): Unit = {
      deserializeStream.close()
      tmpFile.recall()
    }

    context.addTaskCompletionListener(_ => cleanup())
  }

  private def getNextUpstream(spilledFile: SpilledFile): Iterator[(K, C)] = {
    new MapIterator(spilledFile.file)
  }

  private def spillInMemoryIterator(inMemoryIterator: Iterator[(K, C)]): SpilledFile = {

    val spillTmpFile = storageFactory.makeSpillFile()
    val blockId = TempLocalBlockId(spillTmpFile.uuid())
    val spillMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics
    val writer = new SplashObjectWriter(
      blockId,
      spillTmpFile,
      serializer,
      fileBufferSize,
      spillMetrics)

    var objectsWritten = 0

    var success = false
    try {
      while (inMemoryIterator.hasNext) {
        val kv = inMemoryIterator.next()
        writer.write(kv)
        objectsWritten += 1
      }
      writer.close()
      _bytesSpilled += writer.committedPosition
      success = true
    } finally {
      if (!success || objectsWritten == 0) {
        writer.revertPartialWritesAndClose()
        spillTmpFile.recall()
      }
    }

    SpilledFile(spillTmpFile, blockId, success)
  }

  def destructiveIterator(inMemoryIterator: Iterator[(K, C)]): Iterator[(K, C)] = {
    readingIterator = new SplashSpillableIterator[(K, C)](
      inMemoryIterator,
      spillInMemoryIterator,
      getNextUpstream)
    readingIterator
  }

  /** Convenience function to hash the given (K, C) pair by the key. */
  private def hashKey(kc: (K, C)): Int = SplashUtils.hash(kc._1)

  override def toString(): String = {
    s"${getClass.getName}@${Integer.toHexString(hashCode())}"
  }
}

private[spark] object SplashAppendOnlyMap {
  var spilledCount: Int = 0
}

/*
 * Modifications copyright (C) 2018 MemVerge Inc.
 *
 * Modified to use the IO interface class.
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

import java.util.Comparator

import com.memverge.splash.TmpShuffleFile
import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection._

import scala.collection.mutable.ArrayBuffer

private[spark] class SplashSorter[K, V, C](
    context: TaskContext,
    aggregator: Option[Aggregator[K, V, C]] = None,
    partitioner: Option[Partitioner] = None,
    ordering: Option[Ordering[K]] = None,
    serializer: SplashSerializer = SplashSerializer.defaultSerializer())
    extends Spillable[WritablePartitionedPairCollection[K, C]](context.taskMemoryManager())
    with Logging {

  private val conf = SparkEnv.get.conf

  private val splashAggregatorOpt = aggregator.map(new SplashAggregator(_))
  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  private val shouldPartition = numPartitions > 1

  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }

  private val spillCheckInterval = Math.max(
    Math.min(conf.get(SplashOpts.spillCheckInterval),
      conf.get(SplashOpts.forceSpillElements) / 10), 1)

  // Data structures to store in-memory objects before we spill. Depending on whether we have an
  // Aggregator set, we either put objects into an AppendOnlyMap where we combine them, or we
  // store them in an array buffer.
  @volatile private var map = new PartitionedAppendOnlyMap[K, C]
  @volatile private var buffer = new PartitionedPairBuffer[K, C]

  private type KCPartitioned = ((Int, K), C)
  private type KCPair = (K, C)
  private type KVPair = (K, V)
  private type KCIterator = Iterator[KCPair]
  private type KCBufferedIterator = BufferedIterator[KCPair]


  def bytesSpilled: Long = (spills ++ forceSpillFiles).map(_.bytesSpilled).sum

  // Peak size of the in-memory data structure observed so far, in bytes
  private var _peakMemoryUsedBytes: Long = 0L

  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  @volatile private var isShuffleSort: Boolean = true
  private val forceSpillFiles = new ArrayBuffer[SpilledFile]
  @volatile private var readingIterator: SplashSpillableIterator[KCPartitioned] = _

  private val keyComparator: Comparator[K] = ordering.getOrElse(new SplashHashComparator[K])

  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || splashAggregatorOpt.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  private val spills = new ArrayBuffer[SpilledFile]

  private[spark] def spillCount: Int = spills.size

  def insertAll(records: Seq[KVPair]): Unit = insertAll(records.iterator)

  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    splashAggregatorOpt match {
      case Some(agg) =>
        val mergeValue = agg.mergeValue
        val createCombiner = agg.createCombiner
        for (kv <- records) {
          addElementsRead()
          val partition = getPartition(kv._1)
          map.changeValue((partition, kv._1), (hadValue: Boolean, oldValue: C) => {
            if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
          })
          maybeSpillCollection(usingMap = true)
        }
        logDebug(s"insert all combined records")
      case None =>
        for (kv <- records) {
          addElementsRead()
          buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
          maybeSpillCollection(usingMap = false)
        }
        logDebug(s"insert all not combined records")
    }
  }

  def updateTaskMetrics(taskMetrics: TaskMetrics = context.taskMetrics()): Unit = {
    taskMetrics.incMemoryBytesSpilled(memoryBytesSpilled)
    taskMetrics.incDiskBytesSpilled(bytesSpilled)
    taskMetrics.incPeakExecutionMemory(peakMemoryUsedBytes)
  }

  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    if (elementsRead % spillCheckInterval == 0) {
      var estimatedSize = 0L
      if (usingMap) {
        estimatedSize = map.estimateSize()
        if (maybeSpill(map, estimatedSize)) {
          map = new PartitionedAppendOnlyMap[K, C]
        }
      } else {
        estimatedSize = buffer.estimateSize()
        if (maybeSpill(buffer, estimatedSize)) {
          buffer = new PartitionedPairBuffer[K, C]
        }
      }

      _peakMemoryUsedBytes = Math.max(estimatedSize, _peakMemoryUsedBytes)
    }
  }

  private def spillIterator(iterator: WritablePartitionedIterator[K, C]) =
    SpilledFile.spill(
      iterator,
      serializer)

  override protected def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    val inMemoryIterator = destructiveSortedWritablePartitionedIterator(
      collection, comparator)
    val spilledFile = spillIterator(inMemoryIterator)
    spills += spilledFile
  }

  private def destructiveSortedWritablePartitionedIterator(
      collection: WritablePartitionedPairCollection[K, C],
      keyComparator: Option[Comparator[K]]): WritablePartitionedIterator[K, C] = {
    val it = collection.partitionedDestructiveSortedIterator(keyComparator)
    WritablePartitionedIterator(it, numPartitions)
  }

  override protected def forceSpill(): Boolean = {
    if (isShuffleSort) {
      false
    } else {
      assert(readingIterator != null)
      readingIterator.spill() match {
        case Some(spillFile) =>
          forceSpillFiles += spillFile
          map = null
          buffer = null
          true
        case _ =>
          false
      }
    }
  }

  private def merge(
      spills: Seq[SpilledFile],
      inMemory: Iterator[KCPartitioned]): Iterator[(Int, KCIterator)] = {
    val readers = spills.map(getSpillReader)
    val inMemBuffered = inMemory.buffered
    (0 until numPartitions).iterator.map { p =>
      val inMemIterator = new IteratorForPartition(p, inMemBuffered)
      val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)
      if (splashAggregatorOpt.isDefined) {
        (p, mergeWithAggregation(
          iterators, splashAggregatorOpt.get.mergeCombiners))
      } else if (ordering.isDefined) {
        (p, mergeSort(iterators, ordering.get))
      } else {
        (p, iterators.iterator.flatten)
      }
    }
  }

  private def mergeSort(
      iterators: Seq[KCIterator],
      comparator: Comparator[K]): KCIterator = {
    Algorithm.mergeSort[KCPair](
      iterators,
      new Ordering[KCPair] {
        override def compare(x: KCPair, y: KCPair): Int =
          comparator.compare(x._1, y._1)
      })
  }

  private def mergeWithAggregation(
      iterators: Seq[KCIterator],
      mergeCombiners: (C, C) => C): KCIterator = {
    ordering match {
      case Some(_) =>
        // We have a total ordering, so the objects with the same key are sequential.
        new KCIterator {
          private val sorted = mergeSort(iterators, keyComparator).buffered

          override def hasNext: Boolean = sorted.hasNext

          override def next(): KCPair = {
            if (!hasNext) {
              throw new NoSuchElementException
            }
            val elem = sorted.next()
            val k = elem._1
            var c = elem._2
            while (sorted.hasNext && sorted.head._1 == k) {
              val pair = sorted.next()
              c = mergeCombiners(c, pair._2)
            }
            (k, c)
          }
        }
      case _ =>
        val iterator: Iterator[KCIterator] = new Iterator[KCIterator] {
          private val sorted = mergeSort(iterators, keyComparator).buffered

          // Buffers reused across elements to decrease memory allocation
          val keys = new ArrayBuffer[K]
          val combiners = new ArrayBuffer[C]

          override def hasNext: Boolean = sorted.hasNext

          override def next(): KCIterator = {
            if (!hasNext) {
              throw new NoSuchElementException
            }
            keys.clear()
            combiners.clear()
            val firstPair = sorted.next()
            keys += firstPair._1
            combiners += firstPair._2
            val key = firstPair._1
            while (sorted.hasNext && keyComparator.compare(sorted.head._1, key) == 0) {
              val pair = sorted.next()
              var i = 0
              var foundKey = false
              while (i < keys.size && !foundKey) {
                if (keys(i) == pair._1) {
                  combiners(i) = mergeCombiners(combiners(i), pair._2)
                  foundKey = true
                }
                i += 1
              }
              if (!foundKey) {
                keys += pair._1
                combiners += pair._2
              }
            }

            // Note that we return an iterator of elements since we could've had many keys marked
            // equal by the partial order; we flatten this below to get a flat iterator of (K, C).
            keys.iterator.zip(combiners.iterator)
          }
        }
        iterator.flatten
    }

  }

  def destructiveIterator(
      memoryIterator: Iterator[KCPartitioned]): Iterator[KCPartitioned] = {
    if (isShuffleSort) {
      memoryIterator
    } else {
      readingIterator = new SplashSpillableIterator(
        memoryIterator,
        spillInMemoryIterator,
        spilledFile => getSpillReader(spilledFile).iterator())
      readingIterator
    }
  }

  private def spillInMemoryIterator(upstream: Iterator[KCPartitioned]): SpilledFile = {
    val inMemoryIterator = WritablePartitionedIterator(upstream, numPartitions)
    val spilledFile = spillIterator(inMemoryIterator)
    forceSpillFiles += spilledFile
    spilledFile
  }

  def partitionedIterator: Iterator[(Int, KCIterator)] = {
    val usingMap = splashAggregatorOpt.isDefined
    val collection = if (usingMap) map else buffer
    if (spills.isEmpty) {
      groupByPartition(
        destructiveIterator(
          collection.partitionedDestructiveSortedIterator(ordering)))
    } else {
      merge(spills, destructiveIterator(
        collection.partitionedDestructiveSortedIterator(comparator)))
    }
  }

  def iterator: KCIterator = {
    isShuffleSort = false
    partitionedIterator.flatMap(pair => pair._2)
  }

  def toSet: Set[(Int, Set[KCPair])] = {
    partitionedIterator.map(p => (p._1, p._2.toSet)).toSet
  }

  def toSeq: Seq[KCPair] = iterator.toSeq

  def writePartitionedFile(
      blockId: BlockId,
      tmpShuffleFile: TmpShuffleFile): Array[Long] = {
    val lengths = new Array[Long](numPartitions)
    val writer = new SplashObjectWriter(
      blockId,
      tmpShuffleFile,
      serializer,
      context.taskMetrics().shuffleWriteMetrics)

    if (spills.isEmpty) {
      val collection = if (splashAggregatorOpt.isDefined) map else buffer
      val it = destructiveSortedWritablePartitionedIterator(collection, comparator)
      while (it.hasNext) {
        val partitionId = it.partitionId
        while (it.hasNext && it.partitionId == partitionId) {
          it.writeNext(writer)
        }
        lengths(partitionId) = writer.commitAndGet()
      }
    } else {
      for ((id, elements) <- this.partitionedIterator) {
        if (elements.hasNext) {
          for (elem <- elements) {
            writer.write(elem._1, elem._2)
          }
          lengths(id) = writer.commitAndGet()
        }
      }
    }

    writer.close()
    updateTaskMetrics()
    lengths
  }

  def stop(): Unit = {
    logDebug("Stop SpillSorter and clear " +
        s"${spills.length + forceSpillFiles.length} spills.")
    (spills ++ forceSpillFiles).foreach(_.file.delete())
    spills.clear()
    forceSpillFiles.clear()
    if (map != null || buffer != null) {
      map = null
      buffer = null
      releaseMemory()
    }
  }

  def completionIterator: CompletionIterator[KCPair, KCIterator] =
    CompletionIterator[KCPair, KCIterator](iterator, stop())

  private def groupByPartition(data: Iterator[KCPartitioned]): Iterator[(Int, KCIterator)] = {
    val buffered = data.buffered
    (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
  }

  private class IteratorForPartition(
      partitionId: Int, data: BufferedIterator[KCPartitioned]) extends KCIterator {
    override def hasNext: Boolean = data.hasNext && data.head._1._1 == partitionId

    override def next(): KCPair = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val elem = data.next()
      (elem._1._2, elem._2)
    }
  }

  private def getSpillReader(spilledFile: SpilledFile): PartitionedSpillReader[K, C] = {
    new PartitionedSpillReader(spilledFile, serializer)
  }
}

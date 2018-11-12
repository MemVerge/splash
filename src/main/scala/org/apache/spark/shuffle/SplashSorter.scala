/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle

import java.io.{BufferedInputStream, InputStream}
import java.util.{Comparator, UUID}

import com.google.common.io.ByteStreams
import com.memverge.splash.{StorageFactoryHolder, TmpShuffleFile}
import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.DeserializationStream
import org.apache.spark.storage.{BlockId, TempShuffleBlockId}
import org.apache.spark.util.collection._

import scala.collection.mutable
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

  private val storageFactory = StorageFactoryHolder.getFactory
  private val splashAggregator = aggregator.map(new SplashAggregator(_))
  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  private val shouldPartition = numPartitions > 1

  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val fileBufferSize = conf.get(SplashOpts.shuffleFileBufferKB).toInt * 1024

  // Size of object batches when reading/writing from serializers.
  //
  // Objects are written in batches, with each batch using its own serialization stream. This
  // cuts down on the size of reference-tracking maps constructed when deserializing a stream.
  //
  // NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
  // grow internal data structures by growing + copying every time the number of objects doubles.
  private val serializerBatchSize = conf.getLong("spark.shuffle.spill.batchSize", 10000)

  // Data structures to store in-memory objects before we spill. Depending on whether we have an
  // Aggregator set, we either put objects into an AppendOnlyMap where we combine them, or we
  // store them in an array buffer.
  @volatile private var map = new PartitionedAppendOnlyMap[K, C]
  @volatile private var buffer = new PartitionedPairBuffer[K, C]

  private var _bytesSpilled = 0L

  type KCPartitioned = ((Int, K), C)
  type KCIterator = Iterator[Product2[K, C]]
  type KCBufferedIterator = BufferedIterator[Product2[K, C]]


  def bytesSpilled: Long = _bytesSpilled

  // Peak size of the in-memory data structure observed so far, in bytes
  private var _peakMemoryUsedBytes: Long = 0L

  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  @volatile private var isShuffleSort: Boolean = true
  private val forceSpillFiles = new ArrayBuffer[SpilledFile]
  @volatile private var readingIterator: SplashSpillableIterator[KCPartitioned] = _

  private val keyComparator: Comparator[K] = ordering.getOrElse(new SplashHashComparator[K])

  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || splashAggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  private val spills = new ArrayBuffer[SpilledFile]

  private[spark] def numSpills: Int = spills.size

  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    val shouldCombine = splashAggregator.isDefined

    var count = 0
    if (shouldCombine) {
      val mergeValue = splashAggregator.get.mergeValue
      val createCombiner = splashAggregator.get.createCombiner
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        addElementsRead()
        kv = records.next()
        val partition = getPartition(kv._1)
        map.changeValue((partition, kv._1), update)
        maybeSpillCollection(usingMap = true)
        count += 1
      }
      logDebug(s"insert all combined $count records")
    } else {
      for (kv <- records) {
        addElementsRead()
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
        maybeSpillCollection(usingMap = false)
        count += 1
      }
      logDebug(s"insert all not combined $count records")
    }
  }

  private def maybeSpillCollection(usingMap: Boolean): Unit = {
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

    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
  }

  /** @inheritdoc*/
  override protected def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    val inMemoryIterator = destructiveSortedWritablePartitionedIterator(
      collection, comparator)
    val spilledFile = spillMemoryIterator(inMemoryIterator)
    spills += spilledFile
  }

  private def destructiveSortedWritablePartitionedIterator(
      collection: WritablePartitionedPairCollection[K, C],
      keyComparator: Option[Comparator[K]]): WritablePartitionedIterator[K, C] = {
    val it = collection.partitionedDestructiveSortedIterator(keyComparator)
    WritablePartitionedIterator(it)
  }

  /** @inheritdoc*/
  override protected def forceSpill(): Boolean = {
    if (isShuffleSort) {
      false
    } else {
      assert(readingIterator != null)
      readingIterator.spill() match {
        case Some(_) =>
          map = null
          buffer = null
          true
        case _ =>
          false
      }
    }
  }

  /**
   * Spill contents of in-memory iterator to a temporary file on storage.
   */
  private def spillMemoryIterator(
      inMemoryIterator: WritablePartitionedIterator[K, C]): SpilledFile = {
    val spillFile = storageFactory.makeSpillFile()

    var objectsWritten: Long = 0
    val spillMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics
    val blockId = TempShuffleBlockId(spillFile.uuid)
    val writer = new SplashObjectWriter(
      blockId,
      spillFile,
      serializer,
      fileBufferSize,
      spillMetrics)

    val batchSizes = new ArrayBuffer[Long]
    val elementsPerPartition = new Array[Long](numPartitions)

    def flush(): Unit = {
      val len = writer.commitAndGet()
      batchSizes += len
      _bytesSpilled += len
      objectsWritten = 0
    }

    var success = false
    try {
      while (inMemoryIterator.hasNext) {
        val partitionId = inMemoryIterator.nextPartition()
        require(partitionId >= 0 && partitionId < numPartitions,
          s"partition Id: $partitionId should be in the range [0, $numPartitions)")
        inMemoryIterator.writeNext(writer)
        elementsPerPartition(partitionId) += 1
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {
          flush()
        }
      }
      if (objectsWritten > 0) {
        flush()
      } else {
        writer.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (success) {
        writer.close()
      } else {
        writer.revertPartialWritesAndClose()
        spillFile.delete()
      }
    }

    SpilledFile(spillFile,
      TempShuffleBlockId(UUID.randomUUID()),
      success,
      batchSizes.toArray,
      elementsPerPartition)
  }

  private def merge(
      spills: Seq[SpilledFile],
      inMemory: Iterator[KCPartitioned]): Iterator[(Int, KCIterator)] = {
    val readers = spills.map(new SpillReader(_))
    val inMemBuffered = inMemory.buffered
    (0 until numPartitions).iterator.map { p =>
      val inMemIterator = new IteratorForPartition(p, inMemBuffered)
      val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)
      if (splashAggregator.isDefined) {
        (p, mergeWithAggregation(
          iterators, splashAggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
      } else if (ordering.isDefined) {
        (p, mergeSort(iterators, ordering.get))
      } else {
        (p, iterators.iterator.flatten)
      }
    }
  }

  private def mergeSort(
      iterators: Seq[KCIterator],
      comparator: Comparator[K]): Iterator[Product2[K, C]] = {
    val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
    val heap = new mutable.PriorityQueue[KCBufferedIterator]()(new Ordering[KCBufferedIterator] {
      // Use the reverse of comparator.compare because PriorityQueue dequeues the max
      override def compare(x: KCBufferedIterator, y: KCBufferedIterator): Int =
        -comparator.compare(x.head._1, y.head._1)
    })
    heap.enqueue(bufferedIters: _*) // Will contain only the iterators with hasNext = true
    new KCIterator {
      override def hasNext: Boolean = heap.nonEmpty

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val firstBuf = heap.dequeue()
        val firstPair = firstBuf.next()
        if (firstBuf.hasNext) {
          heap.enqueue(firstBuf)
        }
        firstPair
      }
    }
  }

  private def mergeWithAggregation(
      iterators: Seq[KCIterator],
      mergeCombiners: (C, C) => C,
      comparator: Comparator[K],
      totalOrder: Boolean): KCIterator = {
    if (!totalOrder) new Iterator[KCIterator] {
      private val sorted = mergeSort(iterators, comparator).buffered

      // Buffers reused across elements to decrease memory allocation
      val keys = new ArrayBuffer[K]
      val combiners = new ArrayBuffer[C]

      override def hasNext: Boolean = sorted.hasNext

      override def next(): Iterator[Product2[K, C]] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        keys.clear()
        combiners.clear()
        val firstPair = sorted.next()
        keys += firstPair._1
        combiners += firstPair._2
        val key = firstPair._1
        while (sorted.hasNext && comparator.compare(sorted.head._1, key) == 0) {
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
    }.flatMap(i => i) else {
      // We have a total ordering, so the objects with the same key are sequential.
      new KCIterator {
        val sorted: KCBufferedIterator = mergeSort(iterators, comparator).buffered

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Product2[K, C] = {
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
    }
  }

  private class SpillReader(spill: SpilledFile) {
    private val batchOffsets = spill.serializerBatchSizes.scanLeft(0L)(_ + _)

    var partitionId = 0
    var indexInPartition = 0L
    var batchId = 0
    var indexInBatch = 0
    var lastPartitionId = 0

    var inputStream: InputStream = _
    private var deserializeStream = nextBatchStream()

    var nextItem: (K, C) = _
    var finished = false

    skipToNextPartition()

    def nextBatchStream(): DeserializationStream = {
      if (batchId < batchOffsets.length - 1) {
        if (deserializeStream != null) {
          deserializeStream.close()
          deserializeStream = null
        }
        if (inputStream != null) {
          inputStream.close()
          inputStream = null
        }

        val start = batchOffsets(batchId)
        inputStream = spill.file.makeInputStream()
        inputStream.skip(start)
        batchId += 1

        val end = batchOffsets(batchId)

        val batchOffsetsStr = batchOffsets.mkString("[", ", ", "]")
        logDebug(s"start = $start, end = $end, batchOffsets = $batchOffsetsStr")

        val bufferedStream = new BufferedInputStream(
          ByteStreams.limit(inputStream, end - start), fileBufferSize)

        serializer.deserializeStream(spill.blockId, bufferedStream)
      } else {
        cleanup()
        null
      }
    }

    private def skipToNextPartition(): Unit = {
      while (partitionId < numPartitions &&
          indexInPartition == spill.elementsPerPartition(partitionId)) {
        partitionId += 1
        indexInPartition = 0L
      }
    }

    private def readNextItem(): (K, C) = {
      if (finished || deserializeStream == null) {
        null
      } else {
        val k = deserializeStream.readKey().asInstanceOf[K]
        val c = deserializeStream.readValue().asInstanceOf[C]
        lastPartitionId = partitionId
        indexInBatch += 1
        if (indexInBatch == serializerBatchSize) {
          indexInBatch = 0
          deserializeStream = nextBatchStream()
        }
        indexInPartition += 1
        skipToNextPartition()
        if (partitionId == numPartitions) {
          finished = true
          if (deserializeStream != null) {
            deserializeStream.close()
          }
        }
        (k, c)
      }
    }

    var nextPartitionToRead = 0

    def readNextPartition(): KCIterator = new KCIterator {
      private val myPartition = nextPartitionToRead
      nextPartitionToRead += 1

      override def hasNext: Boolean = {
        if (nextItem == null) {
          nextItem = readNextItem()
          if (nextItem == null) {
            return false
          }
        }
        assert(lastPartitionId >= myPartition)
        lastPartitionId == myPartition
      }

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val item = nextItem
        nextItem = null
        item
      }
    }

    def cleanup(): Unit = {
      batchId = batchOffsets.length
      val ds = deserializeStream
      val is = inputStream
      deserializeStream = null
      inputStream = null
      if (ds != null) {
        ds.close()
      }
      if (is != null) {
        is.close()
      }
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
        getNextUpstream)
      readingIterator
    }
  }

  def partitionedIterator: Iterator[(Int, KCIterator)] = {
    val usingMap = splashAggregator.isDefined
    val collection = if (usingMap) map else buffer
    if (spills.isEmpty) {
      if (ordering.isEmpty) {
        groupByPartition(destructiveIterator(collection.partitionedDestructiveSortedIterator(None)))
      } else {
        groupByPartition(destructiveIterator(
          collection.partitionedDestructiveSortedIterator(Some(keyComparator))))
      }
    } else {
      merge(spills, destructiveIterator(
        collection.partitionedDestructiveSortedIterator(comparator)))
    }
  }

  def iterator: KCIterator = {
    isShuffleSort = false
    partitionedIterator.flatMap(pair => pair._2)
  }

  def toSet: Set[(Int, Set[Product2[K, C]])] = {
    partitionedIterator.map(p => (p._1, p._2.toSet)).toSet
  }

  def toSeq: Seq[Product2[K, C]] = {
    iterator.toSeq
  }

  def writePartitionedFile(
      blockId: BlockId,
      tmpShuffleFile: TmpShuffleFile): Array[Long] = {
    val lengths = new Array[Long](numPartitions)
    val writer = new SplashObjectWriter(
      blockId,
      tmpShuffleFile,
      serializer,
      fileBufferSize,
      context.taskMetrics().shuffleWriteMetrics)

    if (spills.isEmpty) {
      val collection = if (splashAggregator.isDefined) map else buffer
      val it = destructiveSortedWritablePartitionedIterator(collection, comparator)
      while (it.hasNext) {
        val partitionId = it.nextPartition()
        while (it.hasNext && it.nextPartition() == partitionId) {
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
    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(bytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)

    lengths
  }

  def stop(): Unit = {
    logDebug("Stop SpillSorter and clear " +
        s"${spills.length + forceSpillFiles.length} spills.")
    spills.foreach(s => s.file.delete())
    spills.clear()
    forceSpillFiles.foreach(s => s.file.delete())
    forceSpillFiles.clear()
    if (map != null || buffer != null) {
      map = null
      buffer = null
      releaseMemory()
    }
  }

  private def groupByPartition(data: Iterator[KCPartitioned]): Iterator[(Int, KCIterator)] = {
    val buffered = data.buffered
    (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
  }

  private class IteratorForPartition(
      partitionId: Int, data: BufferedIterator[KCPartitioned]) extends KCIterator {
    override def hasNext: Boolean = data.hasNext && data.head._1._1 == partitionId

    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val elem = data.next()
      (elem._1._2, elem._2)
    }
  }

  private def spillInMemoryIterator(upstream: Iterator[KCPartitioned]): SpilledFile = {
    val inMemoryIterator = WritablePartitionedIterator(upstream)
    val spilledFile = spillMemoryIterator(inMemoryIterator)
    forceSpillFiles += spilledFile
    spilledFile
  }

  private def getNextUpstream(spilledFile: SpilledFile): Iterator[KCPartitioned] = {
    val spillReader = new SpillReader(spilledFile)
    (0 until numPartitions).iterator.flatMap { p =>
      val iterator = spillReader.readNextPartition()
      iterator.map(cur => ((p, cur._1), cur._2))
    }
  }
}

private[spark] case class WritablePartitionedIterator[K, V](it: Iterator[((Int, K), V)]) {
  private[this] var cur = if (it.hasNext) it.next() else null

  def writeNext(writer: SplashObjectWriter): Unit = {
    writer.write(cur._1._2, cur._2)
    cur = if (it.hasNext) it.next() else null
  }

  def hasNext: Boolean = cur != null

  def nextPartition(): Int = cur._1._1
}

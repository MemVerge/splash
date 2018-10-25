/*
 * Modifications copyright (C) 2018 MemVerge Corp
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
package org.apache.spark.shuffle.sort

import java.util.UUID

import com.memverge.splash.StorageFactoryHolder
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryConsumer
import org.apache.spark.shuffle.{ShuffleSpillInfo, SplashObjectWriter, SplashOpts, SplashSerializer}
import org.apache.spark.storage.TempShuffleBlockId
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.LongArray
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkEnv, TaskContext}

import scala.collection.mutable.ArrayBuffer

/**
 * An external sorter that is specialized for sort-based shuffle.
 * <p>
 * Incoming records are appended to data pages. When all records have been inserted (or when the
 * current thread's shuffle memory limit is reached), the in-memory records are sorted according to
 * their partition ids (using a ShuffleInMemorySorter). The sorted records are then
 * written to a single output file (or multiple files, if we've spilled). The format of the output
 * files is the same as the format of the final output file written by
 * SplashShuffleWriter: each output partition's records are
 * written as a single serialized, compressed stream that can be read with a new decompression and
 * deserialization stream.
 * <p>
 * Unlike SplashSorter, this sorter does not merge its
 * spill files. Instead, this merging is performed in SplashUnsafeShuffleWriter, which uses a
 * specialized merge procedure that avoids extra serialization/deserialization.
 */
private[spark] class SplashUnsafeSorter(
    context: TaskContext,
    numPartitions: Int,
    conf: SparkConf = SparkEnv.get.conf,
    serializer: SplashSerializer
) extends MemoryConsumer(
  context.taskMemoryManager(),
  Math.min(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, context.taskMemoryManager().pageSizeBytes).toInt,
  context.taskMemoryManager().getTungstenMemoryMode) with Logging {

  private val storageFactory = StorageFactoryHolder.getFactory

  private[this] val taskMemoryManager = context.taskMemoryManager()
  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  private val initialSize: Int = conf.get(SplashOpts.shuffleInitialBufferSize)
  private val numElementsForSpillThreshold: Int = conf.get(SplashOpts.forceSpillElements)
  private val useRadixSort = conf.get(SplashOpts.useRadixSort)
  private val fileBufferSize = conf.get(SplashOpts.shuffleFileBufferKB).toInt * 1024

  private var inMemSorter =
    new ShuffleInMemorySorter(this, initialSize, useRadixSort)

  private val allocatedPages = new ArrayBuffer[MemoryBlock]()
  private val spills = new ArrayBuffer[ShuffleSpillInfo]()

  private var peakMemoryUsedBytes = getMemoryUsage
  private var currentPage: MemoryBlock = _
  private var pageCursor = -1L
  private var spilled = 0

  /**
   * Sorts the in-memory records and writes the sorted records to a file.
   * This method does not free the sort data structures.
   *
   * @param isLastFile if true, this indicates that we're writing the final
   *                   output file and that the bytes written should be
   *                   counted towards shuffle spill metrics rather than
   *                   shuffle write metrics.
   */
  private def writeSortedFile(isLastFile: Boolean): Unit = {
    val writeMetricsToUse = if (isLastFile) writeMetrics else new ShuffleWriteMetrics

    // This call performs the actual sort
    val sortedRecords = inMemSorter.getSortedIterator

    // cache to buffer small writes
    val writeBuffer = new Array[Byte](fileBufferSize)
    val blockId = TempShuffleBlockId(UUID.randomUUID())

    val tmpSpillFile = storageFactory.makeSpillFile()
    val spilledFile = ShuffleSpillInfo(
      new Array[Long](numPartitions), tmpSpillFile, blockId)

    val writer = new SplashObjectWriter(
      blockId,
      tmpSpillFile,
      serializer,
      fileBufferSize,
      writeMetricsToUse)

    var currentPartition = -1
    while (sortedRecords.hasNext) {
      sortedRecords.loadNext()
      val partition = sortedRecords.packedRecordPointer.getPartitionId
      assert(partition >= currentPartition)
      if (partition != currentPartition) {
        // new partition
        if (currentPartition != -1) {
          val written = writer.commitAndGet()
          spilledFile.partitionLengths(currentPartition) = written
        }
        currentPartition = partition
      }

      val recordPointer = sortedRecords.packedRecordPointer.getRecordPointer
      val recordPage = taskMemoryManager.getPage(recordPointer)
      val recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer)
      var dataRemaining = Platform.getInt(recordPage, recordOffsetInPage)
      var recordReadPosition = recordOffsetInPage + 4 // skip record length
      while (dataRemaining > 0) {
        val toTransfer = Math.min(fileBufferSize, dataRemaining)
        Platform.copyMemory(
          recordPage,
          recordReadPosition,
          writeBuffer,
          Platform.BYTE_ARRAY_OFFSET,
          toTransfer)
        writer.write(writeBuffer, 0, toTransfer)
        recordReadPosition += toTransfer
        dataRemaining -= toTransfer
      }
    }

    val written = writer.commitAndGet()
    writer.close()

    // If `writeSortedFile()` was called from `closeAndGetSpills()` and no records were inserted,
    // then the file might be empty. Note that it might be better to avoid calling
    // writeSortedFile() in that case.
    if (currentPartition != -1) {
      spilledFile.partitionLengths(currentPartition) = written
      spills.append(spilledFile)
    } else {
      tmpSpillFile.recall()
    }

    if (!isLastFile) { // i.e. this is a spill file
      // The current semantics of `shuffleRecordsWritten` seem to be that it's updated when records
      // are written to disk, not when they enter the shuffle sorting code. DiskBlockObjectWriter
      // relies on its `recordWritten()` method being called in order to trigger periodic updates to
      // `shuffleBytesWritten`. If we were to remove the `recordWritten()` call and increment that
      // counter at a higher-level, then the in-progress metrics for records written and bytes
      // written would get out of sync.
      //
      // When writing the last file, we pass `writeMetrics` directly to the DiskBlockObjectWriter;
      // in all other cases, we pass in a dummy write metrics to capture metrics, then copy those
      // metrics to the true write metrics here. The reason for performing this copying is so that
      // we can avoid reporting spilled bytes as shuffle write bytes.
      //
      // Note that we intentionally ignore the value of `writeMetricsToUse.shuffleWriteTime()`.
      // Consistent with ExternalSorter, we do not count this IO towards shuffle write time.
      // This means that this IO time is not accounted for anywhere; SPARK-3577 will fix this.
      writeMetrics.incRecordsWritten(writeMetricsToUse.recordsWritten)
      context.taskMetrics().incDiskBytesSpilled(writeMetricsToUse.bytesWritten)
    }
  }

  /**
   * Sort and spill the current records to storage in response to memory pressure.
   */
  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    if (trigger != this || inMemSorter == null || inMemSorter.numRecords() == 0) {
      0L
    } else {
      logInfo(s"Thread ${Thread.currentThread().getId} spilling sort " +
          s"data of ${Utils.bytesToString(getMemoryUsage)} to storage (" +
          s"${spills.size} time(s) so far).")
      writeSortedFile(false)
      val spillSize = freeMemory()
      inMemSorter.reset()
      // Reset the in-memory sorter's pointer array only after freeing up the
      // memory pages holding the records. Otherwise, if the task is over
      // allocated memory, then without freeing the memory pages, we might not
      // be able to get memory for the pointer array.
      context.taskMetrics().incMemoryBytesSpilled(spillSize)
      spilled += 1
      spillSize
    }
  }

  private[spark] def getSpilled: Int = spilled

  private def allocatedPageMemory: Long = {
    allocatedPages.map(_.size()).sum
  }

  private def getMemoryUsage: Long = {
    val inMemSize = if (inMemSorter == null) 0 else inMemSorter.getMemoryUsage
    inMemSize + allocatedPageMemory
  }

  private def updatePeakMemoryUsed(): Unit = {
    val mem = getMemoryUsage
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem
    }
  }

  def getPeakMemoryUsedBytes: Long = {
    updatePeakMemoryUsed()
    peakMemoryUsedBytes
  }

  private def freeMemory() = {
    updatePeakMemoryUsed()
    val memoryFreed = allocatedPages.map { page =>
      freePage(page)
      page.size()
    }.sum
    allocatedPages.clear()
    currentPage = null
    pageCursor = 0
    memoryFreed
  }

  /**
   * Force all memory and spill files to be deleted;
   * called by shuffle error-handling code.
   */
  def cleanupResources(): Unit = {
    freeMemory()
    if (inMemSorter != null) {
      inMemSorter.free()
      inMemSorter = null
    }
    spills.foreach(_.file.delete())
  }

  /**
   * Checks whether there is enough space to insert an additional record in to
   * the sort pointer array and grows the array if additional space is
   * required. If the required space cannot be obtained, then the in-memory
   * data will be spilled to disk.
   */
  private def growPointerArrayIfNecessary(): Unit = {
    assert(inMemSorter != null)
    if (!inMemSorter.hasSpaceForAnotherRecord) {
      val used = inMemSorter.getMemoryUsage
      var array: LongArray = null
      try
        array = allocateArray(used / 8 * 2)
      catch {
        // spark 2.1 raises IllegalArgumentException
        // spark 2.3 raises TooLargePageException
        // both of them extends from RuntimeException
        case _: RuntimeException =>
          spill()
          return
        // spark 2.1 raises OutOfMemoryError
        // spark 2.3 raises SparkOutOfMemoryError
        // both of them are OutOfMemoryError
        case e: OutOfMemoryError =>
          if (!inMemSorter.hasSpaceForAnotherRecord) {
            logError("Unable to grow the pointer array.")
            throw e
          }
          return
      }
      // check if spilling is triggered or not
      if (inMemSorter.hasSpaceForAnotherRecord) {
        freeArray(array)
      } else {
        inMemSorter.expandPointerArray(array)
      }
    }
  }

  /**
   * Allocates more memory in order to insert an additional record. This will
   * request additional memory from the memory manager and spill if the
   * requested memory can not be obtained.
   *
   * @param required the required space in the data page, in bytes, including
   *                 space for storing the record size. This must be less than
   *                 or equal to the page size (records that exceed the page
   *                 size are handled via a different code path which uses
   *                 special overflow pages).
   */
  private def acquireNewPageIfNecessary(required: Int): Unit = {
    if (currentPage == null ||
        pageCursor + required > currentPage.getBaseOffset + currentPage.size()) {
      currentPage = allocatePage(required)
      pageCursor = currentPage.getBaseOffset
      allocatedPages.append(currentPage)
    }
  }

  /**
   * Write a record to the shuffle sorter.
   */
  def insertRecord(recordBase: Object,
      recordOffset: Long,
      length: Int,
      partitionId: Int): Unit = {
    assert(inMemSorter != null)
    if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
      logInfo("spilling data because number of spilledRecords cross " +
          s"the threshold $numElementsForSpillThreshold")
      spill()
    }

    growPointerArrayIfNecessary()
    // Need 4 bytes to store the record length.
    val required = length + 4
    acquireNewPageIfNecessary(required)

    assert(currentPage != null)
    val base = currentPage.getBaseObject
    val recordAddress = taskMemoryManager.encodePageNumberAndOffset(
      currentPage, pageCursor)
    Platform.putInt(base, pageCursor, length)
    pageCursor += 4
    Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length)
    pageCursor += length
    inMemSorter.insertRecord(recordAddress, partitionId)
  }

  /**
   * Close the sorter, causing any buffered data to be sorted and written out
   * to storage.
   *
   * @return metadata for the spill files written by this sorter. If no
   *         records were ever inserted into this sorter, then this will
   *         return an empty array.
   */
  def closeAndGetSpills(): Array[ShuffleSpillInfo] = {
    if (inMemSorter != null) {
      writeSortedFile(true)
      freeMemory()
      inMemSorter.free()
      inMemSorter = null
    }
    spills.toArray
  }
}

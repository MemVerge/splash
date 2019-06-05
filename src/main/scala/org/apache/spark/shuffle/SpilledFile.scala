/*
 * Modifications copyright (C) 2018 MemVerge Inc.
 *
 * Use TmpShuffleFile interface instead of raw file.
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

import com.memverge.splash.{StorageFactoryHolder, TmpShuffleFile}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{BlockId, TempLocalBlockId}

case class SpilledFile(
    file: TmpShuffleFile,
    batchOffsets: Array[Long] = Array[Long](),
    elementsPerPartition: Array[Long] = Array[Long]()) {

  val blockId: BlockId = SpilledFile.blockId(file)

  lazy val bytesSpilled: Long =
    batchOffsets.lastOption.getOrElse(file.getSize)

  lazy val batchSizes: Array[Long] =
    batchOffsets zip batchOffsets.tail map (i => i._2 - i._1)
}


object SpilledFile extends Logging {

  // Size of object batches when reading/writing from serializers.
  //
  // Objects are written in batches, with each batch using its own serialization
  // stream. This cuts down on the size of reference-tracking maps constructed
  // when deserializing a stream.
  //
  // NOTE: Setting this too low can cause excessive copying when serializing,
  // since some serializers grow internal data structures by growing + copying
  // every time the number of objects doubles.
  var serializeBatchSize = 10000L

  private lazy val storageFactory = StorageFactoryHolder.getFactory

  private def blockId(file: TmpShuffleFile): BlockId =
    TempLocalBlockId(file.uuid())

  def spill[K, V](
      iterator: Iterator[(K, V)],
      serializer: SplashSerializer): SpilledFile =
    spill(WritablePairIterator(iterator), serializer)

  def spill[K, V](iterator: WritableIterator[K, V],
      serializer: SplashSerializer): SpilledFile = {
    val file = storageFactory.makeSpillFile()
    val writer = new SplashObjectWriter(
      blockId(file),
      file,
      serializer)

    var success = false
    try {
      while (iterator.hasNext) {
        iterator.writeNext(writer)

        if (writer.notCommittedRecords == serializeBatchSize) {
          writer.commitAndGet()
        }
      }
      success = true
    } finally {
      writer.close()
      if (success) {
        logDebug(s"Spill success: ${file.getPath}, size: ${file.getSize}")
      } else {
        logInfo(s"Spill failed: ${file.getPath}")
      }

      if (!success || writer.recordsWritten == 0) {
        file.recall()
      }
    }

    iterator.getSpill(file, writer.batchOffsets)
  }
}


trait WritableIterator[K, V] {
  def writeNext(writer: SplashObjectWriter): Unit

  def hasNext: Boolean

  def getSpill(file: TmpShuffleFile, batchOffsets: Array[Long]): SpilledFile
}


private[spark] case class WritablePairIterator[K, V](
    it: Iterator[(K, V)])
    extends WritableIterator[K, V] {

  override def writeNext(writer: SplashObjectWriter): Unit = {
    writer.write(it.next)
  }

  override def hasNext: Boolean = it.hasNext

  override def getSpill(
      file: TmpShuffleFile, batchOffsets: Array[Long]): SpilledFile =
    SpilledFile(file, batchOffsets)
}


private[spark] case class WritablePartitionedIterator[K, V](
    it: Iterator[((Int, K), V)], numPartitions: Int)
    extends WritableIterator[K, V] {

  private[this] var cur = if (it.hasNext) it.next() else null

  val elementsPerPartition = new Array[Long](numPartitions)

  override def writeNext(writer: SplashObjectWriter): Unit = {
    writer.write(key, value)
    elementsPerPartition(partitionId) += 1
    cur = if (it.hasNext) it.next() else null
  }

  override def hasNext: Boolean = cur != null

  def partitionId: Int = cur._1._1

  private def key: K = cur._1._2

  private def value: V = cur._2

  override def getSpill(
      file: TmpShuffleFile, batchOffsets: Array[Long]): SpilledFile =
    SpilledFile(file, batchOffsets, elementsPerPartition)
}

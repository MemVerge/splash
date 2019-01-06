/*
 * Modifications copyright (C) 2018 MemVerge Inc.
 *
 * Replace local file IO with Splash IO interface.
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

import com.memverge.splash.TmpShuffleFile
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.TempShuffleBlockId
import org.apache.spark.{SparkEnv, TaskContext}

import scala.collection.JavaConversions._


private[spark] class SplashBypassMergeSortShuffleWriter[K, V](
    resolver: SplashShuffleBlockResolver,
    handle: SplashBypassMergeSortShuffleHandle[K, V],
    mapId: Int,
    taskContext: TaskContext) extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency
  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shuffleId = dep.shuffleId
  private val writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics
  private val conf = SparkEnv.get.conf
  private val fileBufferSize = conf.get(SplashOpts.shuffleFileBufferKB).toInt * 1024

  private var partitionWriters: Array[SplashObjectWriter] = _
  private var partitionLengths: Array[Long] = _
  private var partitionTmpWrites: Array[TmpShuffleFile] = _
  private var mapStatus: MapStatus = _
  private var stopping = false

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    require(partitionWriters == null)
    if (!records.hasNext) {
      val tmpShuffleIdPlusFile = resolver.getDataTmpFile(shuffleId, mapId)
      partitionLengths = new Array[Long](numPartitions)
      resolver.writeIndexFileAndCommit(
        shuffleId, mapId, partitionLengths, tmpShuffleIdPlusFile)
      mapStatus = MapStatus(resolver.blockManagerId, partitionLengths)
    } else {
      val serializer = SplashSerializer(dep)
      val start = System.nanoTime()
      partitionWriters = new Array[SplashObjectWriter](numPartitions)
      partitionTmpWrites = new Array[TmpShuffleFile](numPartitions)
      (0 until numPartitions).foreach(i => {
        val tmpDataFile = resolver.getDataTmpFile(shuffleId, mapId)
        val blockId = TempShuffleBlockId(tmpDataFile.uuid)
        partitionWriters(i) = new SplashObjectWriter(
          blockId,
          tmpDataFile,
          serializer,
          fileBufferSize,
          writeMetrics)
      })

      writeMetrics.incWriteTime(System.nanoTime() - start)
      try {
        while (records.hasNext) {
          val record = records.next()
          val key: K = record._1
          partitionWriters(partitioner.getPartition(key)).write(key, record._2)
        }

        (0 until numPartitions).foreach(i => {
          val writer = partitionWriters(i)
          partitionTmpWrites(i) = writer.file
          writer.close()
        })

        val tmp = resolver.getDataTmpFile(shuffleId, mapId)
        try {
          partitionLengths = writePartitionedFile(tmp)
          resolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp)
        } finally {
          tmp.recall()
        }
      } finally {
        partitionWriters.foreach(writer => {
          writer.close()
          if (writer.file != null) {
            writer.file.recall()
          }
        })
      }
    }
    mapStatus = MapStatus(resolver.blockManagerId, partitionLengths)
  }

  private[spark] def getPartitionLengths = partitionLengths

  private def writePartitionedFile(outputFile: TmpShuffleFile): Array[Long] = {
    if (partitionWriters == null) {
      new Array[Long](numPartitions)
    } else {
      val start = System.nanoTime()
      val lengths = outputFile.merge(partitionTmpWrites.toSeq)
          .map(_.asInstanceOf[Long]).toArray
      partitionTmpWrites = null
      writeMetrics.incWriteTime(System.nanoTime() - start)
      lengths
    }
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      None
    } else {
      stopping = true
      if (success) {
        if (mapStatus == null) {
          throw new IllegalStateException(
            "Cannot call stop(true) without having called write()")
        }
        Option(mapStatus)
      } else {
        if (partitionWriters != null) {
          try {
            partitionWriters.foreach(writer => {
              val file = writer.revertPartialWritesAndClose()
              if (file.exists() && !file.delete()) {
                logWarning(s"Error while deleting file ${file.getPath}")
              }
            })
          } finally {
            partitionWriters = null
          }
        }
        None
      }
    }
  }
}

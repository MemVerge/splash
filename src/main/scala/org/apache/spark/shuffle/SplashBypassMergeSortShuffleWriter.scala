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

import java.io.{FileNotFoundException, IOException}

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.TempShuffleBlockId
import org.apache.spark.{SparkEnv, TaskContext}


private[spark] class SplashBypassMergeSortShuffleWriter[K, V](
    resolver: SplashShuffleBlockResolver,
    handle: SplashBypassMergeSortShuffleHandle[K, V],
    mapId: Int,
    taskContext: TaskContext,
    noEmptyFile: Boolean = false) extends ShuffleWriter[K, V] with Logging {
  private val dep = handle.dependency
  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shuffleId = dep.shuffleId
  private val writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics

  private var partitionWriters: Array[SplashObjectWriter] = _
  private val partitionLengths: Array[Long] = Array.fill(numPartitions)(0L)
  private var stopping = false

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    require(partitionWriters == null)
    if (records.hasNext) {
      val serializer = SplashSerializer(dep)
      val start = System.nanoTime()
      partitionWriters = new Array[SplashObjectWriter](numPartitions)
      (0 until numPartitions).foreach(i => {
        val tmpDataFile = resolver.getDataTmpFile(shuffleId, mapId, i)
        val blockId = TempShuffleBlockId(tmpDataFile.uuid)
        partitionWriters(i) = new SplashObjectWriter(
          blockId,
          tmpDataFile,
          serializer,
          writeMetrics,
          noEmptyFile = noEmptyFile)
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
          val file = writer.file
          writer.close()
          file.commit()
          partitionLengths(i) = writer.committedPosition
        })
      } catch {
        case e: Exception =>
          logError("error writing partition, roll back this task", e)
          (0 until numPartitions).foreach(i => {
            partitionLengths(i) = 0
            try {
              val writer = partitionWriters(i)
              writer.close()
              writer.file.recall()
            } catch {
              case e: Exception =>
                logInfo(s"recall shuffle_${shuffleId}_${mapId}_$i failed", e)
            }

            try {
              resolver.getDataFile(shuffleId, mapId, i).delete()
            } catch {
              case e: Exception =>
                logInfo(s"remove shuffle_${shuffleId}_${mapId}_$i failed", e)
            }
          })
      }
    }
  }

  private[spark] def getPartitionLengths = partitionLengths

  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      None
    } else {
      stopping = true
      if (success) {
        Some(MapStatus(resolver.blockManagerId, partitionLengths))
      } else {
        if (partitionWriters != null) {
          try {
            partitionWriters.foreach(writer => {
              val file = writer.revertPartialWritesAndClose()
              try {
                file.delete()
              } catch {
                case _: FileNotFoundException =>
                  logDebug(s"${file.getPath} not exists, nothing to do")
                case e: IOException =>
                  logWarning(s"Error deleting file ${file.getPath}, " +
                      s"error: ${e.getMessage}")
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

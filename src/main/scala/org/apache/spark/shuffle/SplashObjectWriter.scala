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

import java.io.OutputStream

import com.memverge.splash.{ShuffleFile, TmpShuffleFile}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.storage.BlockId
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer


private[spark] class SplashObjectWriter(
    blockId: BlockId,
    val file: TmpShuffleFile,
    splashSerializer: SplashSerializer = SplashSerializer(),
    writeMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics(),
    noEmptyFile: Boolean = false)
    extends OutputStream with Logging {

  private lazy val mcs = file.makeBufferedManualCloseOutputStream(writeMetrics)

  private var bufferedOs: OutputStream = _
  private var objOs: SerializationStream = _

  val committedPositions: ArrayBuffer[Long] = ArrayBuffer[Long](0)

  def batchOffsets: Array[Long] = committedPositions.toArray

  def committedPosition: Long = committedPositions.lastOption.getOrElse(0L)

  private var committedRecords = 0L

  def notCommittedRecords: Long =
    writeMetrics.recordsWritten - committedRecords

  def recordsWritten: Long = writeMetrics.recordsWritten

  private def currentPosition = mcs.getCount

  private def getObjOut: SerializationStream = {
    if (objOs == null) {
      objOs = splashSerializer.serializeStream(blockId, mcs)
    }
    objOs
  }

  private def getBufferedOs: OutputStream = {
    if (bufferedOs == null) {
      bufferedOs = splashSerializer.wrap(blockId, mcs)
    }
    bufferedOs
  }

  private def closeOs(): Unit = {
    if (objOs != null) {
      Utils.tryWithSafeFinally {
        objOs.close()
      } {
        objOs = null
      }
    }
    if (bufferedOs != null) {
      Utils.tryWithSafeFinally {
        bufferedOs.close()
      } {
        bufferedOs = null
      }
    }
  }

  private def closeResources(): Unit = {
    closeOs()
    mcs.manualClose()
    if (noEmptyFile && committedPosition == 0) {
      file.recall()
    }
  }

  override def close(): Unit = {
    Utils.tryWithSafeFinally {
      if (notCommittedRecords != 0) commitAndGet()
    } {
      closeResources()
    }
  }

  def commitAndGet(): Long = {
    closeOs()

    val committedLen = currentPosition - committedPosition
    writeMetrics.incBytesWritten(committedLen)
    committedRecords = writeMetrics.recordsWritten
    committedPositions += currentPosition
    committedLen
  }

  def revertPartialWritesAndClose(): ShuffleFile = {
    Utils.tryWithSafeFinally {
      closeResources()
    } {
      if (notCommittedRecords != 0) {
        logWarning(s"Should truncate $notCommittedRecords items but cannot.")
      }
    }
    file
  }

  def write(key: Any, value: Any): Unit = {
    doWrite(key, value)
    recordWritten()
  }

  private def doWrite(key: Any, value: Any) = {
    val os = getObjOut
    os.writeKey(key)
    os.writeValue(value)
  }

  def write(kv: Product2[Any, Any]): Unit = {
    doWrite(kv._1, kv._2)
    recordWritten()
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    getBufferedOs.write(kvBytes, offs, len)
    recordWritten()
  }

  private def recordWritten(): Unit = writeMetrics.incRecordsWritten(1)
}

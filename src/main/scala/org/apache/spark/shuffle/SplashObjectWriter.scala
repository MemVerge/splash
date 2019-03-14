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

import java.io.{BufferedOutputStream, OutputStream}

import com.memverge.splash.{ShuffleFile, TmpShuffleFile}
import org.apache.commons.io.output.CountingOutputStream
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.storage.{BlockId, TimeTrackingOutputStream}
import org.apache.spark.util.Utils

private class ManualCloseOutputStream(out: OutputStream, bufferSize: Int)
    extends BufferedOutputStream(out, bufferSize) {
  private var closed = false

  def hasBeenClosed: Boolean = closed

  override def close(): Unit = super.flush()

  override def flush(): Unit = {}

  def manualClose(): Unit = {
    super.close()
    closed = true
  }
}

private[spark] class SplashObjectWriter(
    blockId: BlockId,
    val file: TmpShuffleFile,
    splashSerializer: SplashSerializer = SplashSerializer(),
    bufferSize: Int = 32 * 1024,
    writeMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics(),
    noEmptyFile: Boolean = false)
    extends OutputStream with Logging {

  private var initialized = false

  private val mcs: ManualCloseOutputStream = initialize()
  private var countOs: CountingOutputStream = _
  private var bufferedOs: OutputStream = _
  private var objOs: SerializationStream = _
  private var committedPosition = 0L

  private var numRecordsWritten = 0

  private def initialize(): ManualCloseOutputStream = {
    countOs = new CountingOutputStream(file.makeOutputStream())
    val ts = new TimeTrackingOutputStream(writeMetrics, countOs)
    initialized = true
    new ManualCloseOutputStream(ts, bufferSize)
  }

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

  private def closeResources(): Unit = {
    if (initialized) {
      Utils.tryWithSafeFinally {
        closeOs()
        mcs.manualClose()
        if (noEmptyFile && committedPosition == 0) {
          file.recall()
        }
      } {
        countOs = null
        initialized = false
      }
    }
  }

  override def close(): Unit = {
    if (initialized) {
      Utils.tryWithSafeFinally {
        commitAndGet()
      } {
        closeResources()
      }
    }
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

  private[spark] override def flush(): Unit = {
    if (objOs != null) {
      objOs.flush()
    }
    if (bufferedOs != null) {
      bufferedOs.flush()
    }
  }

  def commitAndGet(): Long = {
    flush()
    closeOs()

    val oldCommitted = committedPosition
    committedPosition = if (countOs != null) countOs.getCount else 0
    val committedLen = committedPosition - oldCommitted
    writeMetrics.incBytesWritten(committedLen)
    numRecordsWritten = 0
    committedLen
  }

  def revertPartialWritesAndClose(): ShuffleFile = {
    Utils.tryWithSafeFinally {
      closeResources()
    } {
      if (numRecordsWritten != 0) {
        logInfo(s"Should truncate $numRecordsWritten items but cannot.")
      }
    }
    file
  }

  def write(key: Any, value: Any): Unit = {
    val os = getObjOut
    os.writeKey(key)
    os.writeValue(value)

    recordWritten()
  }

  def write(kv: (Any, Any)): Unit = {
    write(kv._1, kv._2)
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    val os = getBufferedOs
    os.write(kvBytes, offs, len)

    recordWritten()
  }

  private def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)
  }
}

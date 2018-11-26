/*
 * Copyright (c) 2018  MemVerge Inc.
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
    writeMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics())
    extends OutputStream with Logging {

  private var initialized = false

  private lazy val mcs: ManualCloseOutputStream = initialize()
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

  def  commitAndGet(): Long = {
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
        // TODO, deal with this truncate
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

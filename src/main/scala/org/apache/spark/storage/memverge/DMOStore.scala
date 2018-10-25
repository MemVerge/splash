/*
 * Copyright (c) 2018 MemVerge Inc.
 */
package org.apache.spark.storage.memverge

import java.io._
import java.nio.ByteBuffer

import com.google.common.io.Closeables
import com.memverge.mvfs.dmo.{DMOFile, DMOInputStream, DMOOutputStream}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.memverge.MVFSOpts
import org.apache.spark.storage.BlockId
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBuffer

private[spark] class DMOStore(conf: SparkConf, id: String) extends Logging {
  private val memoryMapThreshold = conf.get(MVFSOpts.memoryMapThreshold)
  private val bufferSize = conf.get(MVFSOpts.mvfsShuffleFileBufferKB).toInt * 1024

  private def getDMOId[T](blockId: BlockId) = {
    s"/$id/$blockId"
  }

  private def getDMOFile[T](blockId: BlockId) = {
    new DMOFile(getDMOId(blockId))
  }

  def contains(blockId: BlockId): Boolean = {
    getDMOFile(blockId).exists()
  }

  def getSize(blockId: BlockId): Long = {
    getDMOFile(blockId).getSize()
  }

  def put(blockId: BlockId)(writeFunc: OutputStream => Unit): Unit = {
    if (contains(blockId)) {
      throw new IllegalStateException(s"block $blockId is already present in dmo store.")
    }
    logDebug(s"attempt to put block $blockId")
    val startTime = System.currentTimeMillis
    val file = getDMOFile(blockId)
    val dmoStream = new DMOOutputStream(file, false, true)
    val outputStream = new BufferedOutputStream(dmoStream, bufferSize)
    var threwException: Boolean = true
    try {
      writeFunc(outputStream)
      threwException = false
    } finally {
      try {
        Closeables.close(outputStream, threwException)
      } finally {
        if (threwException) {
          remove(blockId)
        }
      }
    }
    val duration = System.currentTimeMillis - startTime
    val size = Utils.bytesToString(file.getSize())
    logDebug(s"block ${file.getDmoId} stored as $size bytes on dmo in $duration ms")
  }

  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit = {
    put(blockId) { outputStream: OutputStream =>
      Utils.tryWithSafeFinally {
        for (chunk <- bytes.getChunks()) {
          outputStream.write(chunk.array())
        }
      } {
        outputStream.close()
      }
    }
  }

  def getBytes(blockId: BlockId): ChunkedByteBuffer = {
    val dmoIs = getInputStream(blockId)
    Utils.tryWithSafeFinally {
      val bytes = IOUtils.toByteArray(dmoIs)
      new ChunkedByteBuffer(ByteBuffer.wrap(bytes))
    } {
      dmoIs.close()
    }
  }

  def getInputStream(blockId: BlockId): InputStream = {
    val file = getDMOFile(blockId)
    new BufferedInputStream(new DMOInputStream(file), bufferSize)
  }

  def remove(blockId: BlockId): Boolean = {
    val file = getDMOFile(blockId)
    if (file.exists()) {
      file.forceDelete()
      true
    } else {
      false
    }
  }
}

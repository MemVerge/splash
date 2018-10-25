/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle.memverge

import java.io.{BufferedInputStream, InputStream, OutputStream}

import com.memverge.mvfs.dmo.{DMOInputStream, DMOTmpFile}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer._
import org.apache.spark.storage.{BlockId, TempLocalBlockId}
import org.apache.spark.{ShuffleDependency, SparkEnv}


class DMOSerializer(
    serializerManagerOpt: Option[SerializerManager] = None,
    serializerInstanceOpt: Option[SerializerInstance] = None) {
  private lazy val sparkEnv = SparkEnv.get
  private lazy val fileBufferSize = getFileBufferSize
  private lazy val compressCodec = getCompressCodec

  def wrap(blockId: BlockId, outputStream: OutputStream): OutputStream = {
    serializerManagerOpt match {
      case Some(serializerManager) =>
        serializerManager.wrapStream(blockId, outputStream)
      case None =>
        outputStream
    }
  }

  def wrap(blockId: BlockId, inputStream: InputStream): InputStream = {
    serializerManagerOpt match {
      case Some(serializerManager) =>
        serializerManager.wrapStream(blockId, inputStream)
      case None =>
        inputStream
    }
  }

  def wrap(outputStream: OutputStream,
      compressionCodecOpt: Option[CompressionCodec]): OutputStream = {
    val encryptStream: OutputStream = encrypt(outputStream)
    compressionCodecOpt match {
      case Some(compressionCodec) =>
        compressionCodec.compressedOutputStream(encryptStream)
      case None =>
        encryptStream
    }
  }

  def wrap(inputStream: InputStream): InputStream = {
    wrap(inputStream, compressCodec)
  }

  def serializeStream(outputStream: OutputStream): SerializationStream = {
    serializerInstanceOpt match {
      case Some(serializerInstance) =>
        serializerInstance.serializeStream(outputStream)
      case None =>
        val msg = "Object output stream requires serializer instance."
        throw new IllegalArgumentException(msg)
    }
  }

  def serializeStream(
      blockId: BlockId,
      outputStream: OutputStream): SerializationStream = {
    serializeStream(wrap(blockId, outputStream))
  }

  def deserializeStream(tmpFile: DMOTmpFile): DeserializationStream = {
    val blockId = TempLocalBlockId(tmpFile.uuid())
    val inputStream = new DMOInputStream(tmpFile)
    val bufferedIs = new BufferedInputStream(inputStream, fileBufferSize)
    deserializeStream(blockId, bufferedIs)
  }

  def deserializeStream(
      blockId: BlockId,
      inputStream: InputStream): DeserializationStream = {
    serializerInstanceOpt match {
      case Some(serializerInstance) =>
        serializerInstance.deserializeStream(
          wrap(blockId, inputStream))
      case None =>
        val msg = "Object input stream requires serializer instance."
        throw new IllegalArgumentException(msg)
    }
  }

  def wrap(inputStream: InputStream,
      compressionCodecOpt: Option[CompressionCodec]): InputStream = {
    val encryptStream: InputStream = encrypt(inputStream)
    compressionCodecOpt match {
      case Some(compressionCodec) =>
        compressionCodec.compressedInputStream(encryptStream)
      case None =>
        encryptStream
    }
  }

  private def encrypt(outputStream: OutputStream): OutputStream = {
    serializerManagerOpt match {
      case Some(serializerManager) =>
        serializerManager.wrapForEncryption(outputStream)
      case None =>
        outputStream
    }
  }

  private def encrypt(inputStream: InputStream): InputStream = {
    serializerManagerOpt match {
      case Some(serializerManager) =>
        serializerManager.wrapForEncryption(inputStream)
      case None =>
        inputStream
    }
  }

  private def getFileBufferSize = {
    if (sparkEnv != null) {
      sparkEnv.conf.get(MVFSOpts.mvfsShuffleFileBufferKB).toInt * 1024
    } else {
      MVFSOpts.mvfsShuffleFileBufferKB.defaultValue.get.toInt
    }
  }

  private def isCompressEnabled = {
    if (sparkEnv != null) {
      sparkEnv.conf.get(MVFSOpts.shuffleCompress)
    } else {
      false
    }
  }

  private def getCompressCodec = {
    if (isCompressEnabled) {
      Some(CompressionCodec.createCodec(sparkEnv.conf))
    } else {
      None
    }
  }
}


object DMOSerializer {
  def defaultSerializer(): DMOSerializer = {
    DMOSerializer(SparkEnv.get.serializerManager,
      SparkEnv.get.serializer.newInstance())
  }

  def apply(serializerManager: SerializerManager,
      serializerInstance: SerializerInstance): DMOSerializer = {
    new DMOSerializer(Some(serializerManager), Some(serializerInstance))
  }

  def apply(serializer: Serializer): DMOSerializer = {
    DMOSerializer(SparkEnv.get.serializerManager, serializer.newInstance())
  }

  def apply(serializerManager: SerializerManager): DMOSerializer = {
    new DMOSerializer(Some(serializerManager), None)
  }

  def apply(serializerInstance: SerializerInstance): DMOSerializer = {
    new DMOSerializer(None, Some(serializerInstance))
  }

  def apply(dep: ShuffleDependency[_, _, _]): DMOSerializer = {
    DMOSerializer(SparkEnv.get.serializerManager, dep)
  }

  def apply(serializerManager: SerializerManager,
      dep: ShuffleDependency[_, _, _]): DMOSerializer = {
    DMOSerializer(serializerManager, dep.serializer.newInstance())
  }

  def apply(): DMOSerializer = {
    new DMOSerializer(None, None)
  }
}

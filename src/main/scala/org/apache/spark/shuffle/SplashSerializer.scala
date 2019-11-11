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

import java.io.{InputStream, OutputStream}

import com.memverge.splash.TmpShuffleFile
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer._
import org.apache.spark.storage.{BlockId, TempLocalBlockId}
import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv}


class SplashSerializer(
    serializerManagerOpt: Option[SerializerManager] = None,
    serializerInstanceOpt: Option[SerializerInstance] = None) {
  private lazy val sparkEnv = SparkEnv.get
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

  def deserializeStream(tmpFile: TmpShuffleFile): DeserializationStream = {
    val blockId = TempLocalBlockId(tmpFile.uuid())
    val bufferedIs = tmpFile.makeBufferedInputStream()
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

  def isCompressEnabled = {
    if (sparkEnv != null) {
      sparkEnv.conf.get(SplashOpts.shuffleCompress)
    } else {
      false
    }
  }

  private def isEncryptionEnabled = {
    serializerManagerOpt match {
      case Some(serializerManager) =>
        serializerManager.encryptionEnabled
      case None =>
        false
    }
  }

  def isFastMergeSupported: Boolean = {
    val compressionAllowFastMerge =
      getCompressCodec match {
        case Some(codec) =>
          CompressionCodec.supportsConcatenationOfSerializedStreams(codec)
        case None =>
          true
      }
    !isEncryptionEnabled && compressionAllowFastMerge
  }

  private def getCompressCodec = {
    if (isCompressEnabled) {
      Some(CompressionCodec.createCodec(sparkEnv.conf))
    } else {
      None
    }
  }
}


object SplashSerializer {
  def defaultSerializer(): SplashSerializer = {
    SplashSerializer(SparkEnv.get.serializerManager,
      SparkEnv.get.serializer.newInstance())
  }

  def kryo(conf: SparkConf): SplashSerializer = {
    val kryoSerializer = new KryoSerializer(conf)
    val serializerManager = new SerializerManager(kryoSerializer, conf)
    SplashSerializer(serializerManager, kryoSerializer.newInstance())
  }

  def apply(serializerManager: SerializerManager,
      serializerInstance: SerializerInstance): SplashSerializer = {
    new SplashSerializer(Some(serializerManager), Some(serializerInstance))
  }

  def apply(serializer: Serializer): SplashSerializer = {
    SplashSerializer(SparkEnv.get.serializerManager, serializer.newInstance())
  }

  def apply(serializerManager: SerializerManager): SplashSerializer = {
    new SplashSerializer(Some(serializerManager), None)
  }

  def apply(serializerInstance: SerializerInstance): SplashSerializer = {
    new SplashSerializer(None, Some(serializerInstance))
  }

  def apply(dep: ShuffleDependency[_, _, _]): SplashSerializer = {
    SplashSerializer(SparkEnv.get.serializerManager, dep)
  }

  def apply(serializerManager: SerializerManager,
      dep: ShuffleDependency[_, _, _]): SplashSerializer = {
    SplashSerializer(serializerManager, dep.serializer.newInstance())
  }

  def apply(): SplashSerializer = {
    new SplashSerializer(None, None)
  }
}

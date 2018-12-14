/*
 * Copyright (C) 2018 MemVerge Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.shuffle.local

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode
import java.util.UUID

import com.memverge.splash.TempFolder
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.storage._
import org.apache.spark.util.io.ChunkedByteBuffer
import sun.nio.ch.DirectBuffer

object LocalShuffleUtil extends Logging {
  def toBlockId(filename: String): BlockId = {
    filename match {
      case r".*shuffle_(\d+)${shuffle}_(\d+)${mapper}_(\d+)${reducer}.data" =>
        ShuffleDataBlockId(shuffle.toInt, mapper.toInt, reducer.toInt)
      case r".*shuffle_(\d+)${shuffle}_(\d+)${mapper}_(\d+)${reducer}.index" =>
        ShuffleIndexBlockId(shuffle.toInt, mapper.toInt, reducer.toInt)
      case r".*tmp-(.*)${uuid}" =>
        TempLocalBlockId(UUID.fromString(uuid))
      case _ =>
        throw new IllegalArgumentException(s"Unrecognized block id $filename")
    }
  }

  implicit class Regex(sc: StringContext) {
    def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  def getShuffleFolder(appId: String): String = {
    var folder = diskBlockManagerFolder
    if (StringUtils.isEmpty(folder)) {
      folder = TempFolder.getInstance().getShufflePath(appId)
    }
    folder
  }

  def getTmpFolder(): String = {
    TempFolder.getInstance().getTmpPath
  }

  def diskBlockManagerFolder: String = {
    var folder = ""
    if (isSparkEnvAvailable) {
      val blockManager = SparkEnv.get.blockManager
      if (blockManager != null) {
        val diskManager = blockManager.diskBlockManager
        if (diskManager != null) {
          if (diskManager.localDirs.length > 0) {
            folder = diskManager.localDirs.head.getAbsolutePath
            val file = new File(folder)
            if (!file.exists()) {
              file.mkdirs()
            }
          }
        }
      }
    }
    folder
  }

  def putBlock(tmpFile: File, tgtId: String): Unit = {
    val blockId = toBlockId(tgtId)
    if (isSparkEnvAvailable) {
      val blockManager = SparkEnv.get.blockManager
      val channel = new RandomAccessFile(tmpFile, "r").getChannel
      val mappedBuffer = channel.map(MapMode.READ_ONLY, 0, tmpFile.length())

      blockManager.putBytes(blockId,
        new ChunkedByteBuffer(Array[ByteBuffer](mappedBuffer)),
        StorageLevel.DISK_ONLY)
      channel.close()
      val cleaner = mappedBuffer.asInstanceOf[DirectBuffer].cleaner()
      if (cleaner != null) {
        logDebug(s"release direct buffer of ${tmpFile.getAbsolutePath}")
        cleaner.clean()
      }
      logDebug(s"remove tmp file ${tmpFile.getAbsolutePath}")
      FileUtils.deleteQuietly(tmpFile)
    }
  }

  private def isSparkEnvAvailable: Boolean = SparkEnv.get != null
}

/*
 * Modifications copyright (C) 2018 MemVerge Inc.
 *
 * Use storage factory to create input/output streams and get file instance.
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

import java.io._
import java.nio.file.{FileAlreadyExistsException, Paths}
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

import com.memverge.splash.{ShuffleFile, StorageFactory, StorageFactoryHolder, TmpShuffleFile}
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}


private[spark] class SplashShuffleBlockResolver(
    appId: String,
    fileBufferSizeKB: Int = 8)
    extends ShuffleBlockResolver with Logging {
  StorageFactoryHolder.onApplicationStart()

  def getAppId: String = appId

  val NOOP_REDUCE_ID = 0

  val blockManagerId: BlockManagerId = BlockManagerId(appId, "Splash", 666, None)

  private val storageFactory: StorageFactory = StorageFactoryHolder.getFactory

  private val lockMap = new ConcurrentHashMap[(Int, Int), Object]()

  private val lockSupplier = new java.util.function.Function[(Int, Int), Object]() {
    override def apply(t: (Int, Int)): Object = new Object()
  }

  /** @inheritdoc */
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer =
    throw new UnsupportedOperationException("Not used by Splash.")

  def getDataTmpFile(shuffleId: Int, mapId: Int): TmpShuffleFile = {
    storageFactory.makeDataFile(dataFilename(shuffleId, mapId))
  }

  def getDataFile(shuffleId: Int, mapId: Int): ShuffleFile = {
    storageFactory.getDataFile(dataFilename(shuffleId, mapId))
  }

  private def dataFilename(shuffleId: ShuffleId, mapId: ShuffleId) = {
    new File(s"$shuffleFolder", s"shuffle_${shuffleId}_${mapId}_0.data").toString
  }

  private def indexFilename(shuffleId: ShuffleId, mapId: ShuffleId) = {
    new File(s"$shuffleFolder", s"shuffle_${shuffleId}_${mapId}_0.index").toString
  }

  def getDataFile(shuffleBlockId: ShuffleBlockId): ShuffleFile = {
    getDataFile(shuffleBlockId.shuffleId, shuffleBlockId.mapId)
  }

  def getIndexTmpFile(shuffleId: Int, mapId: Int): TmpShuffleFile = {
    storageFactory.makeIndexFile(indexFilename(shuffleId, mapId))
  }

  def getIndexFile(shuffleId: Int, mapId: Int): ShuffleFile = {
    storageFactory.getIndexFile(indexFilename(shuffleId, mapId))
  }

  def getIndexFile(shuffleBlockId: ShuffleBlockId): ShuffleFile = {
    getIndexFile(shuffleBlockId.shuffleId, shuffleBlockId.mapId)
  }

  def getBlockData(blockId: BlockId): Option[InputStream] = {
    if (blockId.isShuffle) {
      val shuffleBlockId = blockId.asInstanceOf[ShuffleBlockId]
      val indexFile = getIndexFile(shuffleBlockId)
      val dataFile = getDataFile(shuffleBlockId)
      try
        SplashUtils.withResources {
          val indexIs = indexFile.makeInputStream()
          logDebug(s"Shuffle ${indexFile.getPath}, seek ${shuffleBlockId.reduceId * 8L}")
          indexIs.skip(shuffleBlockId.reduceId * 8L)
          new DataInputStream(new BufferedInputStream(indexIs, 16))
        } { indexDataIs =>
          val offset = indexDataIs.readLong()
          val nextOffset = indexDataIs.readLong()
          logDebug(s"got partition of $blockId from $offset to $nextOffset " +
              s"from ${indexFile.getPath} size ${indexFile.getSize}")
          val dataIs = new LimitedInputStream(dataFile.makeInputStream(), nextOffset)
          dataIs.skip(offset)

          if (log.isDebugEnabled) {
            logPartitionMd5(dataFile, offset, nextOffset)
          }

          Some(new BufferedInputStream(dataIs, fileBufferSizeKB * 1024))
        }
      catch {
        case ex: IOException =>
          logError(s"Read file ${blockId.name} failed.", ex)
          None
      }
    } else {
      log.error(s"only works for shuffle block id, current block id: $blockId")
      None
    }
  }

  private def logPartitionMd5(dataFile: ShuffleFile, offset: Long, nextOffset: Long) = {
    logDebug(s"read partition from $offset to $nextOffset " +
        s"in ${dataFile.getPath} size ${dataFile.getSize}.")
    SplashUtils.withResources {
      val is = new LimitedInputStream(dataFile.makeInputStream(), nextOffset)
      is.skip(offset)
      new BufferedInputStream(is)
    } { is =>
      val buf = new Array[Byte]((nextOffset - offset).asInstanceOf[ShuffleId])
      is.read(buf)
      val md: MessageDigest = MessageDigest.getInstance("MD5")
      val theDigest: Array[Byte] = md.digest(buf)
      val str = theDigest.map("%02X" format _).mkString
      logDebug(s"md5 for ${dataFile.getPath} offset $offset, length ${buf.length}: $str")
      Some(is)
    }
  }

  private def getDumpFolder = {
    val env = SparkEnv.get
    var localSplashFolder = ""
    if (env != null) {
      val conf = env.conf
      if (conf != null) {
        localSplashFolder = conf.get(SplashOpts.localSplashFolder)
      }
    }
    val dumpFolder = if (StringUtils.isEmpty(localSplashFolder)) {
      System.getProperty("java.io.tmpdir")
    } else {
      localSplashFolder
    }
    dumpFolder
  }

  def dump(blockId: BlockId): String = {
    val dumpFolder: String = getDumpFolder
    val dumpFilePath = Paths.get(dumpFolder, s"${blockId.name}.dump")
    val dumpFile = dumpFilePath.toFile
    if (dumpFile.exists()) {
      log.info(s"old dump file $dumpFilePath already exists, remove it first.")
      dumpFile.delete()
    }
    SplashUtils.withResources {
      new FileOutputStream(dumpFile)
    } { os =>
      getBlockData(blockId) match {
        case Some(is) =>
          try {
            IOUtils.copy(is, os)
            log.info(s"dump ${blockId.name} to $dumpFilePath success.")
          } finally {
            is.close()
          }
        case _ => log.warn(s"input stream is not available for ${blockId.name}")
      }
    }
    dumpFilePath.toString
  }

  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    getDataFile(shuffleId, mapId) delete()
    getIndexFile(shuffleId, mapId) delete()
  }

  private def createEmptyFile(tmpShuffleFile: TmpShuffleFile): Unit = {
    try {
      tmpShuffleFile.create()
    } catch {
      case ex: FileAlreadyExistsException =>
        logDebug(s"${tmpShuffleFile.getPath} not found: ${ex.getMessage}")
      case ex: IOException =>
        logWarning("failed to create file", ex)
    }
  }

  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: TmpShuffleFile): Unit = {
    val indexTmp = storageFactory.makeIndexFile(indexFilename(shuffleId, mapId))
    var offset = 0L

    if (lengths.length == 0 || lengths.sum == 0) {
      // even if there is nothing to write,
      // we need to make sure the tmp files are created.
      createEmptyFile(indexTmp)
      createEmptyFile(dataTmp)
    } else {
      SplashUtils.withResources(
        new DataOutputStream(
          new BufferedOutputStream(
            indexTmp.makeOutputStream()))) { os =>
        os.writeLong(offset)
        for (length <- lengths) {
          offset += length
          os.writeLong(offset)
        }
      }
    }

    var indexFileOpt: Option[ShuffleFile] = None
    lockMap.computeIfAbsent((shuffleId, mapId), lockSupplier).synchronized {
      // check data in the shuffle output that already exists
      val existingLengths = checkIndexAndDataFile(shuffleId, mapId)
      if (existingLengths != null) {
        val blocks = lengths.length
        System.arraycopy(existingLengths, 0, lengths, 0, blocks)
        // data file is available and correct
        dataTmp.recall()
        indexTmp.recall()
      } else {
        dataTmp.commit()
        indexFileOpt = Some(indexTmp.commit())
        logDebug(s"commit shuffle index: " +
            s"${indexTmp.getCommitTarget.getPath}, " +
            s"size: ${indexTmp.getCommitTarget.getSize}")
        if (log.isDebugEnabled()) {
          // check data in the shuffle output we just committed
          checkIndexAndDataFile(shuffleId, mapId)
        }
      }
    }
  }

  def writeShuffle(shuffleId: Int,
      mapId: Int,
      indices: Array[Long],
      data: Array[Byte]): Unit = {
    val indexFile = getIndexTmpFile(shuffleId, mapId)
    val dataFile = getDataTmpFile(shuffleId, mapId)

    try {
      writeIndices(indexFile, indices)
      writeData(dataFile, data)
    } catch {
      case e: Exception =>
        indexFile.recall()
        dataFile.recall()
        throw e
    }

    indexFile.commit()
    dataFile.commit()
  }

  def checkIndexAndDataFile(shuffleId: Int, mapId: Int): Array[Long] = {
    val index = getIndexFile(shuffleId, mapId)
    val data = getDataFile(shuffleId, mapId)
    var ret: Array[Long] = null
    // the index file should have `block + 1` longs as offset.
    try {
      SplashUtils.withResources(
        new DataInputStream(
          new BufferedInputStream(
            index.makeInputStream()))) { in =>
        val numOfLong = index.getSize / 8
        val offsets = 0L until numOfLong map { _ => in.readLong() }

        if (offsets.nonEmpty) {
          ret = validateData(offsets, data)
        } else {
          logDebug(s"offsets length is zero, ${index.getPath} is empty.")
        }
      }
    } catch {
      case ex@(_: IllegalArgumentException |
               _: FileNotFoundException |
               _: IllegalStateException) =>
        logDebug(s"create input stream failed: ${ex.getMessage}")
      case ex: IOException =>
        logWarning("check index and data file failed", ex)
    }
    ret
  }

  private def validateData(offsets: IndexedSeq[Long], data: ShuffleFile): Array[Long] = {
    var ret: Array[Long] = null

    // calculate lengths from offsets
    val lengths = offsets zip offsets.tail map (i => i._2 - i._1)
    // the size of data file should match with index file
    // first element must be 0
    if (offsets(0) == 0 && data.getSize == lengths.sum) {
      ret = lengths.toArray

      if (log.isDebugEnabled) {
        log.debug("log md5 for {} during shuffle write.", data.getPath)
        // print MD5 for each partition
        (0 to offsets.length - 2).foreach { i =>
          logPartitionMd5(data, offsets(i), offsets(i + 1))
        }
      }
    }
    ret
  }

  def writeData(dataFile: TmpShuffleFile, data: Array[Byte]): Unit = {
    SplashUtils.withResources(
      new BufferedOutputStream(
        dataFile.makeOutputStream())) {
      os => os.write(data)
    }
  }

  def writeIndices(indexFile: TmpShuffleFile, indices: Array[Long]): Unit = {
    SplashUtils.withResources(
      new DataOutputStream(
        new BufferedOutputStream(
          indexFile.makeOutputStream()))) { out =>
      val last = indices.foldLeft(0L) { (acc, curr) =>
        out.writeLong(acc)
        acc + curr
      }
      out.writeLong(last)
    }
  }

  /** @inheritdoc */
  override def stop(): Unit = {}

  private[spark] def shuffleFolder = storageFactory.getShuffleFolder(appId)

  def cleanup(): Unit = {
    logInfo(s"cleanup shuffle folder $shuffleFolder for $appId")
    storageFactory.cleanShuffle(appId)
  }
}

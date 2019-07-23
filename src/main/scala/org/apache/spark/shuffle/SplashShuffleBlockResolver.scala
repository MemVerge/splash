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
import java.nio.file.{FileAlreadyExistsException, Path, Paths}
import java.security.MessageDigest

import com.google.common.annotations.VisibleForTesting
import com.memverge.splash.{ShuffleFile, StorageFactory, StorageFactoryHolder, TmpShuffleFile}
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}


class SplashShuffleBlockResolver(appId: String)
    extends ShuffleBlockResolver with Logging {
  StorageFactoryHolder.onApplicationStart()

  def getAppId: String = appId

  val NOOP_REDUCE_ID = 0

  val blockManagerId: BlockManagerId = BlockManagerId(appId, "Splash", 666, None)

  private val storageFactory: StorageFactory = StorageFactoryHolder.getFactory

  private val shuffleTypeManager = ShuffleTypeManager(this)

  private val shuffleCache = ShuffleCache(this)

  private val traceDataChecksum = log.isTraceEnabled

  def getStorageFactoryClassName: String =
    storageFactory.getClass.getCanonicalName

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer =
    throw new UnsupportedOperationException("Not used by Splash.")

  def getDataTmpFile(shuffleId: Int, mapId: Int, reducerId: Int): TmpShuffleFile =
    storageFactory.makeDataFile(dataFilename(shuffleId, mapId, reducerId))

  def getDataTmpFile(shuffleId: Int, mapId: Int): TmpShuffleFile =
    getDataTmpFile(shuffleId, mapId, NOOP_REDUCE_ID)

  private[shuffle] def getDataFile(
      shuffleId: Int, mapId: Int, reducerId: Int): ShuffleFile =
    storageFactory.getDataFile(dataFilename(shuffleId, mapId, reducerId))

  private[shuffle] def getDataFile(shuffleId: Int, mapId: Int): ShuffleFile =
    getDataFile(shuffleId, mapId, NOOP_REDUCE_ID)

  private def dataFilename(
      shuffleId: ShuffleId, mapId: ShuffleId, reducerId: ShuffleId): String =
    Paths.get(s"$shuffleFolder",
      s"shuffle_$shuffleId",
      s"shuffle_${shuffleId}_${mapId}_$reducerId.data"
    ).toString

  private def indexFilename(shuffleId: ShuffleId, mapId: ShuffleId) =
    Paths.get(s"$shuffleFolder",
      s"shuffle_$shuffleId",
      s"shuffle_${shuffleId}_${mapId}_$NOOP_REDUCE_ID.index"
    ).toString

  private def getDataFile(shuffleBlockId: ShuffleBlockId): ShuffleFile =
    shuffleCache.getDataFile(shuffleBlockId)

  def getIndexTmpFile(shuffleId: Int, mapId: Int): TmpShuffleFile =
    storageFactory.makeIndexFile(indexFilename(shuffleId, mapId))

  def getIndexFile(shuffleId: Int, mapId: Int): ShuffleFile =
    storageFactory.getIndexFile(indexFilename(shuffleId, mapId))

  def getIndexFile(shuffleBlockId: ShuffleBlockId): ShuffleFile =
    getIndexFile(shuffleBlockId.shuffleId, shuffleBlockId.mapId)

  def getBlockData(blockId: BlockId): Option[BlockDataStreamInfo] = {
    if (blockId.isShuffle) {
      val shuffleBlockId = blockId.asInstanceOf[ShuffleBlockId]
      if (shuffleTypeManager.isHashBasedShuffle(shuffleBlockId)) {
        readHashBasedPartition(shuffleBlockId)
      } else {
        readPartitionByIndex(shuffleBlockId)
      }
    } else {
      logError(s"only works for shuffle block id, current block id: $blockId")
      None
    }
  }

  private def readHashBasedPartition(shuffleBlockId: ShuffleBlockId) = {
    val dataFile = getDataFile(
      shuffleBlockId.shuffleId,
      shuffleBlockId.mapId,
      shuffleBlockId.reduceId)

    if (traceDataChecksum) logPartitionMd5(dataFile)

    val stream = dataFile.makeBufferedInputStream()
    Some(BlockDataStreamInfo(stream, dataFile.getSize))
  }

  private def readPartitionByIndex(shuffleBlockId: ShuffleBlockId) = {
    val partitionLoc = shuffleCache.getPartitionLoc(shuffleBlockId)
    partitionLoc match {
      case Some(partition) =>
        val dataFile = getDataFile(shuffleBlockId)

        if (traceDataChecksum) logPartitionMd5(dataFile, partition)

        val stream = dataFile.makeBufferedInputStreamWithin(partition)
        Some(BlockDataStreamInfo(stream, partition.length))
      case None => None
    }
  }

  private def logPartitionMd5(dataFile: ShuffleFile): Some[InputStream] =
    logPartitionMd5(dataFile, 0, dataFile.getSize)

  private def logPartitionMd5(
      dataFile: ShuffleFile, partitionLoc: PartitionLoc): Some[InputStream] =
    logPartitionMd5(dataFile, partitionLoc.start, partitionLoc.end)

  private def logPartitionMd5(
      dataFile: ShuffleFile,
      offset: Long,
      nextOffset: Long): Some[InputStream] = {
    logDebug(s"read partition from $offset to $nextOffset " +
        s"in ${dataFile.getPath} size ${dataFile.getSize}.")
    SplashUtils.withResources {
      val stream = dataFile.makeBufferedInputStreamWithin(offset, nextOffset)
      new BufferedInputStream(stream)
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

  def getDumpFilePath(blockId: BlockId): Path =
    Paths.get(getDumpFolder, s"$blockId.dump")

  def dump(blockId: BlockId): String = {
    val dumpFilePath = getDumpFilePath(blockId)
    val dumpFile = dumpFilePath.toFile
    if (dumpFile.exists()) {
      logInfo(s"old dump file $dumpFilePath already exists, remove it first.")
      dumpFile.delete()
    }
    SplashUtils.withResources {
      new FileOutputStream(dumpFile)
    } { os =>
      getBlockData(blockId) match {
        case Some(BlockDataStreamInfo(is, _)) =>
          try {
            IOUtils.copy(is, os)
            logInfo(s"dump $blockId to $dumpFilePath success.")
          } finally {
            is.close()
          }
        case _ => log.warn(s"input stream is not available for $blockId")
      }
    }
    dumpFilePath.toString
  }

  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    getDataFile(shuffleId, mapId) delete()
    getIndexFile(shuffleId, mapId) delete()
  }

  def invalidateShuffleCache(shuffleId: Int): Unit =
    shuffleCache.invalidateCache(shuffleId)

  private def createEmptyFile(tmpShuffleFile: TmpShuffleFile): Unit = {
    val maxTry = 3
    var tryCount = 0
    var success = false
    while (tryCount < maxTry && !success) {
      tryCount += 1
      try {
        tmpShuffleFile.create()
        success = true
      } catch {
        case ex: FileAlreadyExistsException =>
          logDebug(s"${tmpShuffleFile.getPath} exists, remove and recreate." +
              s"err: ${ex.getMessage}")
          if (tryCount == maxTry) throw ex else tmpShuffleFile.delete()
        case ex: IOException =>
          logWarning("failed to create file", ex)
      }
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
      val dataTmpSize = dataTmp.getSize
      if (dataTmpSize > 0) {
        logWarning(s"data size is $dataTmpSize " +
            s"while lengths is ${lengths.toSeq}, do nothing and return")
        dataTmp.recall()
        dataTmp.delete()
        return
      } else {
        // even if there is nothing to write,
        // we need to make sure the tmp files are created.
        createEmptyFile(indexTmp)
        createEmptyFile(dataTmp)
      }
    } else {
      SplashUtils.withResources(
        new DataOutputStream(
          indexTmp.makeBufferedOutputStream())) { os =>
        os.writeLong(offset)
        for (length <- lengths) {
          offset += length
          os.writeLong(offset)
        }
      }
    }

    commitTmpFiles(lengths, indexTmp, dataTmp)
    if (traceDataChecksum) {
      // check data in the shuffle output we just committed
      checkIndexAndDataFile(shuffleId, mapId)
    }
  }

  private def commitTmpFiles(
      lengths: Array[Long],
      indexTmp: TmpShuffleFile,
      dataTmp: TmpShuffleFile): Unit = {
    try {
      logDebug(s"commit shuffle index ${indexTmp.getCommitTarget.getPath}.")
      indexTmp.commit()

      logDebug(s"commit shuffle data ${dataTmp.getCommitTarget.getPath}.")
      dataTmp.commit()
    } catch {
      case ex: FileAlreadyExistsException =>
        val indexFile = indexTmp.getCommitTarget
        val dataFile = dataTmp.getCommitTarget
        val indexPath = indexFile.getPath
        val dataPath = dataFile.getPath
        logWarning(s"target already exists, ${ex.getMessage}")
        val tmpDataSize = dataTmp.getSize
        if (lengths.sum != tmpDataSize) {
          logInfo("invalid re-commit data, keep the old one, "
              + s"new lengths: ${lengths.toSeq}, data size: $tmpDataSize")
          indexTmp.recall()
          dataTmp.recall()
        } else {
          logInfo("replace disk data with new data, "
              + s"new lengths: ${lengths.toSeq}, data size: $tmpDataSize")
          logDebug(s"delete old index $indexPath")
          indexFile.delete()
          logDebug(s"delete old data $dataPath")
          dataFile.delete()
          logDebug(s"commit new shuffle index $indexPath.")
          indexTmp.commit()
          logDebug(s"commit new shuffle data $dataPath.")
          dataTmp.commit()
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

    logDebug(s"write shuffle index ${indexFile.getCommitTarget.getPath}.")
    indexFile.commit()

    logDebug(s"write shuffle data ${indexFile.getCommitTarget.getPath}.")
    dataFile.commit()
  }

  def checkIndexAndDataFile(shuffleId: Int, mapId: Int): Array[Long] = {
    val index = getIndexFile(shuffleId, mapId)
    val data = getDataFile(shuffleId, mapId)
    var ret: Array[Long] = null
    // the index file should have `block + 1` longs as offset.
    try {
      val offsets = readIndex(shuffleId, mapId)
      if (offsets.nonEmpty) {
        ret = validateData(offsets, data)
      } else {
        logDebug(s"offsets length is zero, ${index.getPath} is empty.")
      }
    } catch {
      case ex: IOException =>
        logWarning("check index and data file failed", ex)
    }
    ret
  }

  private[shuffle] def readIndex(shuffleId: Int, mapId: Int): Array[Long] = {
    val index = getIndexFile(shuffleId, mapId)
    try {
      SplashUtils.withResources(index.makeBufferedDataInputStream()) { in =>
        Iterator.continually {
          try {
            in.readLong()
          } catch {
            case _: EOFException => -1
          }
        }.takeWhile(_ >= 0).toArray
      }
    } catch {
      case ex@(_: IllegalArgumentException |
               _: IOException |
               _: IllegalStateException) =>
        logDebug(s"create input stream failed: ${ex.getMessage}")
        Array.emptyLongArray
    }
  }

  private def validateData(offsets: IndexedSeq[Long], data: ShuffleFile): Array[Long] = {
    var ret: Array[Long] = null

    // the size of data file should match with index file
    // first element must be 0
    if (offsets(0) == 0 && data.getSize == offsets.last) {
      // calculate lengths from offsets
      ret = (offsets zip offsets.tail map (i => i._2 - i._1)).toArray

      if (traceDataChecksum) {
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
    SplashUtils.withResources(dataFile.makeBufferedOutputStream()) {
      os => os.write(data)
    }
  }

  def writeIndices(indexFile: TmpShuffleFile, indices: Array[Long]): Unit = {
    SplashUtils.withResources(indexFile.makeBufferedDataOutputStream()) { out =>
      val last = indices.foldLeft(0L) { (acc, curr) =>
        out.writeLong(acc)
        acc + curr
      }
      out.writeLong(last)
    }
  }

  override def stop(): Unit = shuffleCache.invalidateAll()

  def shuffleFolder: String = storageFactory.getShuffleFolder(appId)

  def cleanup(): Unit = {
    logInfo(s"cleanup shuffle folder $shuffleFolder for $appId")
    storageFactory.cleanShuffle(appId)
  }

  @VisibleForTesting
  def putShuffleBlock(shuffleId: Int, mapId: Int, lengths: Array[Long],
      dataOpt: Option[Array[Byte]] = None): ShuffleBlockId = {
    val data = dataOpt match {
      case Some(arr) => arr
      case None => (1L to lengths.sum).map(_.toByte).toArray
    }

    val dataTmp = getDataTmpFile(shuffleId, mapId)
    writeData(dataTmp, data)
    writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)
    ShuffleBlockId(shuffleId, mapId, 0)
  }
}

case class ShuffleTypeManager(resolver: SplashShuffleBlockResolver)
    extends Logging {

  private val hashShuffleSet = collection.mutable.Set[Int]()
  private val sortShuffleSet = collection.mutable.Set[Int]()

  def addHashShuffle(shuffleId: Int): Unit = hashShuffleSet.add(shuffleId)

  def addSortShuffle(shuffleId: Int): Unit = sortShuffleSet.add(shuffleId)

  def isHashBasedShuffle(shuffleBlockId: ShuffleBlockId): Boolean = {
    val shuffleId = shuffleBlockId.shuffleId
    if (hashShuffleSet.contains(shuffleId)) {
      true
    } else if (sortShuffleSet.contains(shuffleId)) {
      false
    } else {
      if (!resolver.getIndexFile(shuffleBlockId).exists()) {
        logDebug(s"add $shuffleId to hash based shuffle set")
        hashShuffleSet.add(shuffleId)
      } else {
        logDebug(s"add $shuffleId to sort based shuffle set")
        sortShuffleSet.add(shuffleId)
      }
      isHashBasedShuffle(shuffleBlockId)
    }
  }
}

case class BlockDataStreamInfo(is: InputStream, length: Long)

case class PartitionLoc(start: Long, end: Long) {
  lazy val length: Long = end - start
}
/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package org.apache.spark.shuffle

import java.io._
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

import com.memverge.splash.{ShuffleFile, StorageFactory, StorageFactoryHolder, TmpShuffleFile}
import org.apache.commons.lang3.NotImplementedException
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}


private[spark] class SplashShuffleBlockResolver(
    appId: String,
    fileBufferSizeKB: Int = 8)
    extends ShuffleBlockResolver with Logging {

  def getAppId: String = appId

  val NOOP_REDUCE_ID = 0

  val blockManagerId: BlockManagerId = BlockManagerId(appId, "Splash", 666, None)

  private val storageFactory: StorageFactory = StorageFactoryHolder.getFactory

  private val lockMap = new ConcurrentHashMap[(Int, Int), Object]()

  private val lockSupplier = new java.util.function.Function[(Int, Int), Object]() {
    override def apply(t: (Int, Int)): Object = new Object()
  }

  /** @inheritdoc*/
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer =
    throw new NotImplementedException("Not used by Splash.")

  def getDataTmpFile(shuffleId: Int, mapId: Int): TmpShuffleFile = {
    storageFactory.makeDataFile(dataFilename(shuffleId, mapId))
  }

  def getDataFile(shuffleId: Int, mapId: Int): ShuffleFile = {
    storageFactory.getDataFile(dataFilename(shuffleId, mapId))
  }

  private def dataFilename(shuffleId: ShuffleId, mapId: ShuffleId) = {
    new File(s"$shuffleFolder",s"shuffle_${shuffleId}_${mapId}_0.data").toString
  }

  private def indexFilename(shuffleId: ShuffleId, mapId: ShuffleId) = {
    new File(s"$shuffleFolder",s"shuffle_${shuffleId}_${mapId}_0.index").toString
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
          logDebug(s"Shuffle ${indexFile.getId}, seek ${shuffleBlockId.reduceId * 8L}")
          indexIs.skip(shuffleBlockId.reduceId * 8L)
          new DataInputStream(new BufferedInputStream(indexIs, 16))
        } { indexDataIs =>
          val offset = indexDataIs.readLong()
          val nextOffset = indexDataIs.readLong()
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
      logDebug(s"md5 for ${dataFile.getId} offset $offset, length ${buf.length}: $str")
      Some(is)
    }
  }

  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    getDataFile(shuffleId, mapId) delete()
    getIndexFile(shuffleId, mapId) delete()
  }

  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: TmpShuffleFile): Unit = {
    val indexTmp = storageFactory.makeIndexFile(indexFilename(shuffleId, mapId))
    var offset = 0L

    SplashUtils.withResources(
      new DataOutputStream(
        new BufferedOutputStream(
          indexTmp.makeOutputStream(true)))) { os =>
      os.writeLong(offset)
      for (length <- lengths) {
        offset += length
        os.writeLong(offset)
      }
    }

    var indexFileOpt: Option[ShuffleFile] = None
    lockMap.computeIfAbsent((shuffleId, mapId), lockSupplier).synchronized {
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
            s"${indexTmp.getCommitTarget.getId}, " +
            s"size: ${indexTmp.getCommitTarget.getSize}")
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
    if (data.exists() && index.exists()) {
      SplashUtils.withResources(
        new DataInputStream(
          new BufferedInputStream(
            index.makeInputStream()))) { in =>
        val numOfLong = index.getSize / 8
        val offsets = 0L until numOfLong map { _ => in.readLong() }

        // calculate lengths from offsets
        val lengths = offsets zip offsets.tail map (i => i._2 - i._1)
        // the size of data file should match with index file
        // first element must be 0
        if (offsets(0) == 0 && data.getSize == lengths.sum) {
          ret = lengths.toArray
        }
      }
    }
    ret
  }

  def writeData(dataFile: ShuffleFile, data: Array[Byte]): Unit = {
    SplashUtils.withResources(
      new BufferedOutputStream(
        dataFile.makeOutputStream(true))) {
      os => os.write(data)
    }
  }

  def writeIndices(indexFile: ShuffleFile, indices: Array[Long]): Unit = {
    SplashUtils.withResources(
      new DataOutputStream(
        new BufferedOutputStream(
          indexFile.makeOutputStream(true)))) { out =>
      val last = indices.foldLeft(0L) { (acc, curr) =>
        out.writeLong(acc)
        acc + curr
      }
      out.writeLong(last)
    }
  }

  /** @inheritdoc*/
  override def stop(): Unit = {}

  private[spark] def shuffleFolder = storageFactory.getShuffleFolder(appId)

  def cleanup(): Unit = {
    storageFactory.getDataFile(shuffleFolder).delete()
  }
}

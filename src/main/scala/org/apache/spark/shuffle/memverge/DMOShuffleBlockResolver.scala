/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package org.apache.spark.shuffle.memverge

import java.io._
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

import com.memverge.mvfs.connection.MVFSConnectOptions
import com.memverge.mvfs.dmo.{DMOFile, DMOInputStream, DMOOutputStream, DMOTmpFile}
import org.apache.commons.lang3.NotImplementedException
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}


private[spark] class DMOShuffleBlockResolver(
    appId: String,
    socket: String = MVFSConnectOptions.defaultSocket(),
    indexChunkSizeKB: Int = 1,
    dataChunkSizeKB: Int = 1024,
    fileBufferSizeKB: Int = 8)
    extends ShuffleBlockResolver with Logging {

  def getAppId: String = appId

  val NOOP_REDUCE_ID = 0

  val blockManagerId: BlockManagerId = BlockManagerId(appId, "dmo", getSocket, None)

  private val indexChunkSize = indexChunkSizeKB * 1024
  private val dataChunkSize = dataChunkSizeKB * 1024

  private val lockMap = new ConcurrentHashMap[(Int, Int), Object]()

  private val lockSupplier = new java.util.function.Function[(Int, Int), Object]() {
    override def apply(t: (Int, Int)): Object = new Object()
  }

  /**
   * Get Integer of the socket used.  Note if socket is "0", we will
   * set it to `Int.MaxValue`.  The reason is spark does not allow port
   * smaller than 1.
   *
   * @return Integer version of the socket string.
   */
  def getSocket: Int = {
    val intMax = Int.MaxValue
    try {
      val ret = socket.toInt
      if (ret == 0) {
        intMax
      } else {
        ret
      }
    } catch {
      case _: NumberFormatException => intMax
    }
  }

  def getSocketString: String = socket

  /** @inheritdoc*/
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer =
    throw new NotImplementedException("Not used by DMO implementation.")

  def getDataTmpFile(socket: String): DMOTmpFile = {
    DMOTmpFile.make(socket, dataChunkSize)
  }

  def getDataTmpFile(shuffleId: Int, mapId: Int): DMOTmpFile = {
    DMOTmpFile.make(getDataFile(shuffleId, mapId))
  }

  def getDataFile(shuffleId: Int, mapId: Int): DMOFile = {
    val filename = s"/$appId/shuffle_${shuffleId}_${mapId}_0.data"
    new DMOFile(socket, filename, dataChunkSize)
  }

  def getDataFile(shuffleBlockId: ShuffleBlockId): DMOFile = {
    getDataFile(shuffleBlockId.shuffleId, shuffleBlockId.mapId)
  }

  def getIndexTmpFile(shuffleId: Int, mapId: Int): DMOTmpFile = {
    DMOTmpFile.make(getIndexFile(shuffleId, mapId))
  }

  def getIndexFile(shuffleId: Int, mapId: Int): DMOFile = {
    val filename = s"/$appId/shuffle_${shuffleId}_${mapId}_0.index"
    new DMOFile(socket, filename, indexChunkSize)
  }

  def getIndexFile(shuffleBlockId: ShuffleBlockId): DMOFile = {
    getIndexFile(shuffleBlockId.shuffleId, shuffleBlockId.mapId)
  }

  def getBlockData(blockId: BlockId): Option[InputStream] = {
    if (blockId.isShuffle) {
      val shuffleBlockId = blockId.asInstanceOf[ShuffleBlockId]
      val indexFile = getIndexFile(shuffleBlockId)
      val dataFile = getDataFile(shuffleBlockId)
      try
        DMOUtils.withResources {
          val dmoIndexIs = new DMOInputStream(indexFile)
          logDebug(s"Shuffle ${indexFile.getDmoId}, seek ${shuffleBlockId.reduceId * 8L}")
          dmoIndexIs.seek(shuffleBlockId.reduceId * 8L)
          new DataInputStream(new BufferedInputStream(dmoIndexIs, 16))
        } { indexDataIs =>
          val offset = indexDataIs.readLong()
          val nextOffset = indexDataIs.readLong()
          val dmoDataIs = new DMOInputStream(dataFile, nextOffset)
          dmoDataIs.seek(offset)
          dmoDataIs.prefetch()

          if (log.isDebugEnabled) {
            logPartitionMd5(dataFile, offset, nextOffset)
          }

          Some(new BufferedInputStream(dmoDataIs, fileBufferSizeKB * 1024))
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

  private def logPartitionMd5(dataFile: DMOFile, offset: Long, nextOffset: Long) = {
    DMOUtils.withResources {
      val is = new DMOInputStream(dataFile, nextOffset)
      is.seek(offset)
      new BufferedInputStream(is)
    } { is =>
      val buf = new Array[Byte]((nextOffset - offset).asInstanceOf[ShuffleId])
      is.read(buf)
      val md: MessageDigest = MessageDigest.getInstance("MD5")
      val theDigest: Array[Byte] = md.digest(buf)
      val str = theDigest.map("%02X" format _).mkString
      logDebug(s"md5 for ${dataFile.getDmoId} offset $offset, length ${buf.length}: $str")
      Some(is)
    }
  }

  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    getDataFile(shuffleId, mapId) forceDelete()
    getIndexFile(shuffleId, mapId) forceDelete()
  }

  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: DMOTmpFile): Unit = {
    val indexTmp = DMOTmpFile.make(getIndexFile(shuffleId, mapId))
    var offset = 0L

    DMOUtils.withResources(
      new DataOutputStream(
        new BufferedOutputStream(
          new DMOOutputStream(indexTmp, false, true)))) { os =>
      os.writeLong(offset)
      for (length <- lengths) {
        offset += length
        os.writeLong(offset)
      }
    }

    var indexFileOpt: Option[DMOFile] = None
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
        logDebug(s"commit DMO shuffle index: " +
            s"${indexTmp.getCommitTarget.getDmoId}, " +
            s"size: ${indexTmp.getCommitTarget.getSize}")
      }
    }
    indexFileOpt.foreach(_.broadcast())
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
      DMOUtils.withResources(
        new DataInputStream(
          new BufferedInputStream(
            new DMOInputStream(index)))) { in =>
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

  def writeData(dataFile: DMOFile, data: Array[Byte]): Unit = {
    DMOUtils.withResources(
      new BufferedOutputStream(new DMOOutputStream(dataFile, false, true))) {
      os => os.write(data)
    }
  }

  def writeIndices(indexFile: DMOFile, indices: Array[Long]): Unit = {
    DMOUtils.withResources(
      new DataOutputStream(
        new BufferedOutputStream(
          new DMOOutputStream(indexFile, false, true)))) { out =>
      val last = indices.foldLeft(0L) { (acc, curr) =>
        out.writeLong(acc)
        acc + curr
      }
      out.writeLong(last)
    }
  }

  /** @inheritdoc*/
  override def stop(): Unit = {}

  def cleanup(): Unit = {
    new DMOFile(socket, s"/$appId").forceDelete()
  }
}

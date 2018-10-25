/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle.memverge

import java.io.{FileNotFoundException, InputStream}

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

/**
 * An iterator that fetches blocks from DMO.
 *
 * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
 * in a pipelined fashion as they are received.
 *
 * @param resolver      DMO block resolver of the shuffle
 * @param shuffleBlocks list of blocks to fetch.
 *                      For each block we also require the size (in bytes as a long field) in
 *                      order to filter zero blocks
 */
private[spark] class DMOShuffleFetcherIterator(
    resolver: DMOShuffleBlockResolver,
    shuffleBlocks: Seq[(BlockId, Long)])
    extends Iterator[(BlockId, InputStream, Int)] with Logging {

  private val blockIds = shuffleBlocks
      .filter(_._2 > 0) // only retrieve blocks not empty
      .map(_._1)

  private val inputIterator = getInputs

  override def hasNext: Boolean = inputIterator.hasNext

  override def next(): (BlockId, InputStream, Int) = inputIterator.next()

  private def getInputs = {
    blockIds.iterator.flatMap { blockId =>
      try {
        resolver.getBlockData(blockId) match {
          case Some(inputStream) =>
            val length = inputStream.available()
            logDebug(s"Read shuffle ${blockId.name}, length: $length")
            Some((blockId, inputStream, length))
          case None =>
            val msg = s"Failed to load block ${blockId.name}."
            logError(msg)
            throw new FileNotFoundException(msg)
        }
      } catch {
        case e: Exception =>
          val msg = s"Failed to get block ${blockId.name}, which is not a " +
              s"shuffle block.  Skip $blockId."
          logError(msg, e)
          None
      }
    }
  }
}

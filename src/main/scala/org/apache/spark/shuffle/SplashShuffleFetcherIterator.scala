/*
 * Modifications copyright (C) 2018 MemVerge Corp
 *
 * Extracted from ShuffleReader.
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

import java.io.{FileNotFoundException, InputStream}

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

/**
 * An iterator that fetches blocks from storage.
 *
 * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
 * in a pipelined fashion as they are received.
 *
 * @param resolver      block resolver of the shuffle
 * @param shuffleBlocks list of blocks to fetch.
 *                      For each block we also require the size (in bytes as a long field) in
 *                      order to filter zero blocks
 */
private[spark] class SplashShuffleFetcherIterator(
    resolver: SplashShuffleBlockResolver,
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

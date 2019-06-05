/*
 * Modifications copyright (C) 2018 MemVerge Inc.
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

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.DeserializationStream
import org.apache.spark.storage.BlockId
import org.apache.spark.util.CompletionIterator

/**
 * An iterator that fetches blocks from storage.
 *
 * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
 * in a pipelined fashion as they are received.
 *
 * @param resolver      block resolver of the shuffle
 * @param shuffleBlocks list of blocks to fetch.
 */
private[spark] case class SplashShuffleFetcherIterator(
    resolver: SplashShuffleBlockResolver,
    shuffleBlocks: Iterator[BlockId])
    extends Iterator[SplashShuffleFetcher] with Logging {
  require(shuffleBlocks != null)
  require(shuffleBlocks != null)

  private lazy val inputIterator = shuffleBlocks.flatMap { blockId =>
    try {
      resolver.getBlockData(blockId) match {
        case Some(BlockDataStreamInfo(inputStream, length)) =>
          logDebug(s"Read shuffle $blockId, length: $length, " +
              s"open stream: $inputStream")
          Some(FetcherImpl(blockId, inputStream, length.intValue))
        case None =>
          throw new FileNotFoundException(s"Failed to load block $blockId.")
      }
    } catch {
      case e: Exception =>
        val msg = s"Failed to get block $blockId, which may not be a " +
            s"shuffle block.  Skip it."
        logError(msg, e)
        None
    }
  }

  override def hasNext: Boolean = inputIterator.hasNext

  override def next(): SplashShuffleFetcher = inputIterator.next()

  private case class FetcherImpl(
      blockId: BlockId,
      stream: InputStream,
      length: Int) extends SplashShuffleFetcher with Logging {
    private def deserializeStream(
        serializer: SplashSerializer): DeserializationStream =
      serializer.deserializeStream(blockId, stream)

    private def asKeyValueIterator(serializer: SplashSerializer): PairIterator =
      deserializeStream(serializer).asKeyValueIterator

    def asMetricIterator(
        serializer: SplashSerializer,
        taskMetrics: TaskMetrics): CompletionIterator[Pair, PairIterator] = {
      val readMetrics = taskMetrics.createTempShuffleReadMetrics()
      readMetrics.incLocalBlocksFetched(1)
      readMetrics.incLocalBytesRead(length)

      val keyValueIterator = asKeyValueIterator(serializer)

      val dumpIterator = IteratorOnErrorWrapper(keyValueIterator,
        () => {
          logInfo(s"error in block: $blockId, size: $length, offset: $offset")
          resolver.dump(blockId)
          close()
        })

      CompletionIterator[Pair, PairIterator](
        dumpIterator.map { record =>
          readMetrics.incRecordsRead(1)
          record
        }, {
          close()
          taskMetrics.mergeShuffleReadMetrics()
        })
    }

    def close(): Unit = {
      logDebug(s"close stream $stream")
      stream.close()
    }

    def offset: Int = length - stream.available()
  }

}


trait SplashShuffleFetcher {
  type Pair = (_, _)
  type PairIterator = Iterator[Pair]

  def blockId: BlockId

  def stream: InputStream

  def length: Int

  def offset: Int

  def close(): Unit

  def asMetricIterator(
      serializer: SplashSerializer,
      taskMetrics: TaskMetrics): CompletionIterator[Pair, PairIterator]
}


case class IteratorOnErrorWrapper[+A](sub: Iterator[A], f: () => Unit)
    extends Iterator[A] with Logging {
  def next(): A = try {
    sub.next()
  } catch {
    case e: Exception =>
      logError(s"error during iterator.next, ${e.getMessage}")
      f()
      throw e
  }

  def hasNext: Boolean = try {
    sub.hasNext
  } catch {
    case e: Exception =>
      logError(s"error during iterator.hasNext, ${e.getMessage}")
      f()
      throw e
  }
}

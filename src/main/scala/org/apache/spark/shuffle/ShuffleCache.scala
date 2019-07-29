/*
 * Modifications copyright (C) 2019 MemVerge Inc.
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

import java.io.{DataInputStream, EOFException}
import java.util.concurrent.TimeUnit

import com.memverge.splash.ShuffleFile
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.storage.ShuffleBlockId
import org.spark_project.guava.cache.{CacheBuilder, CacheLoader, LoadingCache}

import scala.collection.mutable


case class ShuffleCache(resolver: SplashShuffleBlockResolver)
    extends Logging {

  private lazy val shuffleMap = CacheBuilder.newBuilder()
      .maximumSize(getCacheSize)
      .expireAfterAccess(1, TimeUnit.MINUTES)
      .build(new CacheLoader[Int, DataMap] {
        override def load(shuffleId: Int): DataMap =
          DataMap(resolver, shuffleId)
      })
      .asInstanceOf[LoadingCache[Int, DataMap]]

  def getDataFile(blockId: ShuffleBlockId): ShuffleFile =
    getDataMap(blockId.shuffleId).getDataFile(blockId)

  def getPartitionLoc(blockId: ShuffleBlockId): Option[PartitionLoc] = {
    try {
      val shuffleId = blockId.shuffleId
      val dataMap = getDataMap(shuffleId)
      dataMap.getPartitionLoc(blockId)
    } catch {
      case e: Exception =>
        logError(s"error reading partition location for $blockId", e)
        None
    }
  }

  def invalidateCache(shuffleId: Int): Unit = {
    shuffleMap.get(shuffleId).invalidate()
    shuffleMap.invalidate(shuffleId)
  }

  def invalidateAll(): Unit = shuffleMap.invalidateAll()

  private def getDataMap(shuffleId: Int) = shuffleMap.get(shuffleId)

  private def getCacheSize: Int = {
    val env = SparkEnv.get
    val cacheSizeOpt = SplashOpts.shuffleCacheSize
    val invalidSize = -1
    var cacheSize: Int = invalidSize
    if (env != null) {
      val conf = env.conf
      if (conf != null) {
        cacheSize = conf.get(cacheSizeOpt)
      }
    }
    if (cacheSize == invalidSize) {
      cacheSize = cacheSizeOpt.defaultValue.get
    }
    logInfo(s"initialize shuffle cache with max size $cacheSize")
    cacheSize
  }
}


case class DataMap(resolver: SplashShuffleBlockResolver, shuffleId: Int)
    extends Logging {
  private val mapper2Data = CacheBuilder.newBuilder()
      .build(new CacheLoader[Int, Option[Array[Long]]] {
        override def load(mapId: Int): Option[Array[Long]] = {
          val blockId = ShuffleBlockId(shuffleId, mapId, resolver.NOOP_REDUCE_ID)
          initOffsetArray(blockId)
        }
      })
      .asInstanceOf[LoadingCache[Int, Option[Array[Long]]]]

  private val blockId2DataFile = CacheBuilder.newBuilder()
      .build(new CacheLoader[(Int, Int), ShuffleFile] {
        override def load(index: (Int, Int)): ShuffleFile = {
          logDebug("get block data for " +
              s"shuffle ${index._1} map ${index._2}")
          resolver.getDataFile(index._1, index._2)
        }
      })
      .asInstanceOf[LoadingCache[(Int, Int), ShuffleFile]]

  def getDataFile(blockId: ShuffleBlockId): ShuffleFile = {
    val index = (blockId.shuffleId, blockId.mapId)
    blockId2DataFile.get(index)
  }

  def getPartitionLoc(blockId: ShuffleBlockId): Option[PartitionLoc] = {
    val offsetArrayOpt = mapper2Data.get(blockId.mapId)
    val reducerId = blockId.reduceId
    offsetArrayOpt.map(arr => PartitionLoc(arr(reducerId), arr(reducerId + 1)))
  }

  private def initOffsetArray(blockId: ShuffleBlockId): Option[Array[Long]] = {
    val indexFile = resolver.getIndexFile(blockId)
    val ret = mutable.ArrayBuffer[Long]()
    try {
      SplashUtils.withResources {
        logDebug(s"read shuffle index ${indexFile.getPath}")
        new DataInputStream(indexFile.makeBufferedInputStream())
      }(stream =>
        try {
          while (true) {
            ret += stream.readLong()
          }
        } catch {
          case _: EOFException => // do nothing
        }
      )
      Some(ret.toArray)
    } catch {
      case e: Exception =>
        logError(s"read index file ${indexFile.getPath} failed", e)
        None
    }
  }

  def invalidate(): Unit = {
    mapper2Data.invalidateAll()
    blockId2DataFile.invalidateAll()
  }
}

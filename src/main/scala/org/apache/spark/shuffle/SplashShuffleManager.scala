/*
 * Modifications copyright (C) 2018 MemVerge Inc.
 *
 * Replace the original shuffle class with Splash version classes.
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

import java.util.concurrent.ConcurrentHashMap

import com.memverge.splash.StorageFactoryHolder
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.sort.SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE
import org.apache.spark.shuffle.sort.SplashUnsafeShuffleWriter

class SplashShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  StorageFactoryHolder.setSparkConf(conf)
  private val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()

  /**
   * Return a resolver capable of retrieving shuffle block data based on block
   * coordinates.
   */
  override lazy val shuffleBlockResolver = new SplashShuffleBlockResolver(
    conf.getAppId,
    conf.get(SplashOpts.shuffleFileBufferKB).toInt)

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to
   * tasks.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (shouldBypassMergeSort(dependency)) {
      new SplashBypassMergeSortShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]]
      )
    } else if (useSerializedShuffle(dependency)) {
      // Try to buffer map outputs in a serialized form, since this
      // is more efficient:
      new SplashSerializedShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // Buffer map outputs in a deserialized form:
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext): ShuffleWriter[K, V] = {
    numMapsForShuffle.putIfAbsent(
      handle.shuffleId,
      handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)
    handle match {
      case unsafeShuffleHandle: SplashSerializedShuffleHandle[K@unchecked, V@unchecked] =>
        new SplashUnsafeShuffleWriter(
          shuffleBlockResolver,
          unsafeShuffleHandle,
          mapId,
          context,
          SplashSerializer(unsafeShuffleHandle.dependency))
      case bypassShuffleHandle: SplashBypassMergeSortShuffleHandle[K@unchecked, V@unchecked] =>
        new SplashBypassMergeSortShuffleWriter(
          shuffleBlockResolver,
          bypassShuffleHandle,
          mapId,
          context)
      case other: BaseShuffleHandle[K@unchecked, V@unchecked, _] =>
        new SplashShuffleWriter(
          shuffleBlockResolver,
          other,
          mapId,
          context)
    }
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    new SplashShuffleReader(
      shuffleBlockResolver,
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
      startPartition,
      endPartition,
      context)
  }

  /**
   * Remove a shuffle's metadata from the ShuffleManager.
   *
   * @return true if the metadata removed successfully, otherwise false.
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    logInfo(s"unregister shuffle $shuffleId of app ${conf.getAppId}")
    Option(numMapsForShuffle.remove(shuffleId)).foreach { numMaps =>
      if (isDriver && conf.get(SplashOpts.clearShuffleOutput)) {
        logInfo(s"remove shuffle $shuffleId data with $numMaps mappers.")
        (0 until numMaps).foreach { mapId =>
          shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
        }
      }
    }
    true
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    StorageFactoryHolder.onApplicationEnd()
    if (isDriver && conf.get(SplashOpts.clearShuffleOutput)) {
      shuffleBlockResolver.cleanup()
    }
    shuffleBlockResolver.stop()
  }

  private def isDriver = {
    val executorId = SparkEnv.get.executorId
    executorId == SparkContext.DRIVER_IDENTIFIER ||
        executorId == SparkContext.LEGACY_DRIVER_IDENTIFIER
  }

  private def useSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean = {
    val optionKey = "spark.shuffle.splash.useBaseShuffle"
    val useBaseShuffle = conf.getBoolean(optionKey, defaultValue = false)
    !useBaseShuffle && canUseSerializedShuffle(dependency)
  }

  /**
   * Helper method for determining whether a shuffle should use an optimized
   * serialized shuffle path or whether it should fall back to the original
   * path that operates on deserialized objects.
   */
  private def canUseSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean = {
    val shufId = dependency.shuffleId
    val numPartitions = dependency.partitioner.numPartitions
    if (!dependency.serializer.supportsRelocationOfSerializedObjects) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because the serializer, " +
          s"${dependency.serializer.getClass.getName}, does not support object relocation")
      false
    } else if (dependency.aggregator.isDefined) {
      log.debug(
        s"Can't use serialized shuffle for shuffle $shufId because an aggregator is defined")
      false
    } else if (numPartitions > MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because it has more than " +
          s"$MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE partitions")
      false
    } else {
      log.debug(s"Can use serialized shuffle for shuffle $shufId")
      true
    }
  }

  private def shouldBypassMergeSort(dep: ShuffleDependency[_, _, _]): Boolean = {
    if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      false
    } else {
      val bypassMergeThreshold: Int = conf.get(SplashOpts.bypassSortThreshold)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * serialized shuffle.
 */
private[spark] class SplashSerializedShuffleHandle[K, V](
    shuffleId: Int,
    numMaps: Int,
    dependency: ShuffleDependency[K, V, V])
    extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * bypass merge sort shuffle path.
 */
private[spark] class SplashBypassMergeSortShuffleHandle[K, V](
    shuffleId: Int,
    numMaps: Int,
    dependency: ShuffleDependency[K, V, V])
    extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}

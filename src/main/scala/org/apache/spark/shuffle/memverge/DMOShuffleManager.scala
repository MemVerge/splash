/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle.memverge

import java.util.concurrent.ConcurrentHashMap

import com.memverge.mvfs.dmo.FileHandleCache
import com.memverge.mvfs.metrics.DMOMetricsMgr
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.DMOUnsafeShuffleWriter
import org.apache.spark.shuffle.sort.SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE
import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}

class DMOShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  private val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()
  private val dmoMetricsManager = DMOMetricsMgr.getInstance()
  private val jmvfsMetricsEnabled: Boolean = conf.get(MVFSOpts.mvfsEnableJmvfsMetrics)

  dmoMetricsManager.setEnabled(jmvfsMetricsEnabled)

  updateFileHandleCacheSize()

  private def updateFileHandleCacheSize(): Unit = {
    val handleCacheSize = conf.get(MVFSOpts.mvfsHandleCacheSize)
    if (handleCacheSize > 0) {
      FileHandleCache.getInstance().setCacheSize(handleCacheSize)
    }
  }

  /** @inheritdoc */
  override lazy val shuffleBlockResolver = new DMOShuffleBlockResolver(
    conf.getAppId,
    conf.get(MVFSOpts.mvfsSocket),
    conf.get(MVFSOpts.mvfsIndexChunkSize).toInt,
    conf.get(MVFSOpts.mvfsDataChunkSizeKB).toInt,
    conf.get(MVFSOpts.mvfsShuffleFileBufferKB).toInt)

  /** @inheritdoc */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (useSerializedShuffle(dependency)) {
      // Try to buffer map outputs in a serialized form, since this
      // is more efficient:
      new DMOSerializedShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // Buffer map outputs in a deserialized form:
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }

  /** @inheritdoc */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext): ShuffleWriter[K, V] = {
    numMapsForShuffle.putIfAbsent(
      handle.shuffleId,
      handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)
    handle match {
      case unsafeShuffleHandle: DMOSerializedShuffleHandle[K@unchecked, V@unchecked] =>
        new DMOUnsafeShuffleWriter(
          shuffleBlockResolver,
          unsafeShuffleHandle,
          mapId,
          context,
          DMOSerializer(unsafeShuffleHandle.dependency))
      case other: BaseShuffleHandle[K@unchecked, V@unchecked, _] =>
        new DMOShuffleWriter(
          shuffleBlockResolver,
          other,
          mapId,
          context)
    }
  }

  /** @inheritdoc */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    new DMOShuffleReader(
      shuffleBlockResolver,
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
      startPartition,
      endPartition,
      context)
  }

  /** @inheritdoc */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    logInfo(s"unregister shuffle $shuffleId of app ${conf.getAppId}")
    Option(numMapsForShuffle.remove(shuffleId)).foreach { numMaps =>
      if (conf.get(MVFSOpts.mvfsClearShuffleOutput)) {
        logInfo(s"remove shuffle $shuffleId data with $numMaps mappers.")
        (0 until numMaps).foreach { mapId =>
          shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
        }
      }
    }
    true
  }

  /** @inheritdoc */
  override def stop(): Unit = {
    // clear everything in file handle cache
    val socket = shuffleBlockResolver.getSocketString
    FileHandleCache.getInstance().invalidate(socket)

    if (conf.get(MVFSOpts.mvfsClearShuffleOutput)) {
      shuffleBlockResolver.cleanup()
    }
    shuffleBlockResolver.stop()
    if (jmvfsMetricsEnabled) {
      logInfo(s"DMO metrics: \n${dmoMetricsManager.getMetricsString}")
      dmoMetricsManager.reset()
    }
  }

  private def useSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean = {
    val optionKey = "spark.shuffle.mvfs.useBaseShuffle"
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
}

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * serialized shuffle.
 */
private[spark] class DMOSerializedShuffleHandle[K, V](
    shuffleId: Int,
    numMaps: Int,
    dependency: ShuffleDependency[K, V, V])
    extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}

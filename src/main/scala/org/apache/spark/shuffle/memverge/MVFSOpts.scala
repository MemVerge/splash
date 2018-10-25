/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle.memverge

import com.memverge.mvfs.connection.MVFSConnectOptions
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}
import org.apache.spark.network.util.ByteUnit

private[spark] object MVFSOpts {
  // DMO shuffle manager only keys
  val mvfsSocket: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.mvfs.socket")
        .doc("the default socket used to connect to dmo.")
        .stringConf
        .createWithDefaultString(MVFSConnectOptions.defaultSocket())

  val mvfsHandleCacheSize: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.mvfs.handleCacheSize")
        .doc("the size of the file handle/file size cache.")
        .intConf
        .createWithDefault(-1)

  val mvfsDataChunkSizeKB: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.mvfs.data.chunkSize")
        .doc("chunk size of the file used by shuffle data files.")
        .bytesConf(ByteUnit.KiB)
        .createWithDefaultString("1m")

  val mvfsIndexChunkSize: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.mvfs.index.chunkSize")
        .doc("chunk size of the file used by shuffle index files.")
        .bytesConf(ByteUnit.KiB)
        .createWithDefaultString("1k")

  val mvfsClearShuffleOutput: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.mvfs.clearShuffleOutput")
        .doc("clear shuffle output if set to true.")
        .booleanConf
        .createWithDefault(true)

  val mvfsEnableJmvfsMetrics: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.mvfs.jmvfsMetrics")
        .doc("enable metrics at jmvfs side.")
        .booleanConf
        .createWithDefault(false)

  // spark options
  val forceSpillElements: ConfigEntry[Int] =
    createIfNotExists("spark.shuffle.spill.numElementsForceSpillThreshold", builder => {
      builder.intConf.createWithDefault(Int.MaxValue)
    })

  val useRadixSort: ConfigEntry[Boolean] =
    createIfNotExists("spark.shuffle.sort.useRadixSort", builder => {
      builder.booleanConf.createWithDefault(true)
    })

  val fastMergeEnabled: ConfigEntry[Boolean] =
    createIfNotExists("spark.shuffle.unsafe.fastMergeEnabled", builder => {
      builder.booleanConf.createWithDefault(true)
    })

  val shuffleCompress: ConfigEntry[Boolean] =
    createIfNotExists("spark.shuffle.compress", builder => {
      builder.booleanConf.createWithDefault(true)
    })

  val shuffleInitialBufferSize: ConfigEntry[Int] =
    createIfNotExists("spark.shuffle.sort.initialBufferSize", builder => {
      builder
          .doc("Shuffle initial buffer size used by the sorter.")
          .intConf
          .createWithDefault(4096)
    })

  val memoryMapThreshold: ConfigEntry[Long] =
    createIfNotExists("spark.storage.memoryMapThreshold", builder => {
      builder.bytesConf(ByteUnit.BYTE).createWithDefaultString("2m")
    })

  // compatible entries for spark 2.1, scala 2.10, migrated from spark 2.3
  val mvfsShuffleFileBufferKB: ConfigEntry[Long] =
    createIfNotExists("spark.shuffle.file.buffer", builder => {
      builder
          .doc("Size of the in-memory buffer for each shuffle file output stream, in KiB unless " +
              "otherwise specified. These buffers reduce the number of disk seeks and system calls " +
              "made in creating intermediate shuffle files.")
          .bytesConf(ByteUnit.KiB)
          .createWithDefaultString("32k")
    })

  private def createIfNotExists[T](
      optionKey: String,
      f: ConfigBuilder => ConfigEntry[T]): ConfigEntry[T] = {
    val existingEntry: ConfigEntry[_] = ConfigEntry.findEntry(optionKey)
    if (existingEntry != null) {
      existingEntry.asInstanceOf[ConfigEntry[T]]
    } else {
      f(ConfigBuilder(optionKey))
    }
  }
}

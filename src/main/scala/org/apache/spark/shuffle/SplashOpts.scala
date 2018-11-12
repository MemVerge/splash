package org.apache.spark.shuffle

import org.apache.spark.internal.config
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}
import org.apache.spark.network.util.ByteUnit

private[spark] object SplashOpts {
  val storageFactoryName: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.splash.storageFactory")
        .doc("class name of the storage factory to use.")
        .stringConf
        .createWithDefault("com.memverge.splash.local.LocalStorageFactory")

  val clearShuffleOutput: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.splash.clearShuffleOutput")
        .doc("clear shuffle output if set to true.")
        .booleanConf
        .createWithDefault(true)

  // spark options
  val forceSpillElements: ConfigEntry[Int] =
    config.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD

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
  val shuffleFileBufferKB: ConfigEntry[Long] = config.SHUFFLE_FILE_BUFFER_SIZE

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

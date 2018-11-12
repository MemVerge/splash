package org.apache.spark.shuffle

import com.memverge.splash.TmpShuffleFile
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

case class SpilledFile(
    file: TmpShuffleFile,
    blockId: BlockId,
    success: Boolean,
    serializerBatchSizes: Array[Long] = Array[Long](),
    elementsPerPartition: Array[Long] = Array[Long]()) extends Logging {
  if (success) {
    logInfo(s"Spill success: ${file.getId}, size: ${file.getSize}")
  } else {
    logInfo(s"Spill failed: ${file.getId}")
  }
}

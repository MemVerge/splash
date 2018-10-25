/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle.memverge

import com.memverge.mvfs.dmo.DMOTmpFile
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

case class SpilledFile(
    file: DMOTmpFile,
    blockId: BlockId,
    success: Boolean,
    serializerBatchSizes: Array[Long] = Array[Long](),
    elementsPerPartition: Array[Long] = Array[Long]()) extends Logging {
  if (success) {
    logInfo(s"DMO spill success: ${file.getDmoId}, size: ${file.getSize}")
  } else {
    logInfo(s"DMO spill failed: ${file.getDmoId}")
  }
}

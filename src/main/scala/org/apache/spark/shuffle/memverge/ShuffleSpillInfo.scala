/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle.memverge

import com.memverge.mvfs.dmo.DMOTmpFile
import org.apache.spark.storage.TempShuffleBlockId

case class ShuffleSpillInfo(
    partitionLengths: Array[Long],
    file: DMOTmpFile,
    blockId: TempShuffleBlockId) {
}

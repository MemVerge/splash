package org.apache.spark.shuffle

import com.memverge.splash.TmpShuffleFile
import org.apache.spark.storage.TempShuffleBlockId

case class ShuffleSpillInfo(
    partitionLengths: Array[Long],
    file: TmpShuffleFile,
    blockId: TempShuffleBlockId) {
}

/*
 * Copyright (C) 2018 MemVerge Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.shuffle.local

import java.io.{File, InputStream}

import com.memverge.splash.ShuffleFile
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId


class LocalShuffleFile(path: String)
    extends ShuffleFile with Logging {

  private def blockManager = SparkEnv.get.blockManager

  private def master = blockManager.master

  private def blockId: BlockId = LocalShuffleUtil.toBlockId(path)

  def file: File = {
    if (new File(path).exists()) {
      new File(path)
    } else {
      blockManager.diskBlockManager.getFile(blockId)
    }
  }


  override def getSize: Long = {
    if (isLocal) {
      file.length()
    } else {
      master.getBlockStatus(blockId).values
          .filter(status => status.diskSize > 0 || status.memSize > 0)
          .map(status => status.diskSize + status.memSize)
          .headOption
          .getOrElse(0L)
    }
  }

  override def delete(): Boolean = {
    if (isLocal) {
      blockManager.removeBlock(blockId)
    } else {
      master.removeBlock(blockId)
    }
    true
  }

  override def exists(): Boolean = {
    isLocal || master.contains(blockId)
  }

  private def isLocal: Boolean = file.exists()

  override def getPath: String = file.getAbsolutePath

  private def useRemote =
    SparkEnv.get.conf.get(LocalOpts.alwaysUseRemote.key, "false").toBoolean

  override def makeInputStream(): InputStream = {
    if (!useRemote && isLocal) {
      logInfo(s"found $blockId locally,  do local read.")
      blockManager.getLocalBytes(blockId).map(data => {
        logDebug(s"received local ${data.size} bytes for $blockId")
        data.toInputStream()
      }).orNull
    } else {
      logInfo(s"$blockId is a remote block, do remote read.")
      blockManager.getRemoteBytes(blockId)
          .map(data => {
            logDebug(s"received remote ${data.size} bytes for $blockId")
            data.toInputStream(dispose = true)
          })
          .orNull
    }
  }
}

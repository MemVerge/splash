/*
 * Copyright (C) 2018 MemVerge Corp
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

import java.io.{File, FileInputStream, InputStream}

import com.memverge.splash.ShuffleFile
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId


class LocalShuffleFile(path: String)
    extends ShuffleFile with Logging {

  private def blockManager = SparkEnv.get.blockManager

  private def master = blockManager.master

  private def blockId: BlockId = LocalShuffleUtil.toBlockId(path)

  protected def file: File = {
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
      master.getLocationsAndStatus(blockId)
          .map(status => status.status.diskSize)
          .filter(size => size > 0)
          .head
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

  override def getId: String = file.getAbsolutePath

  private def useRemote =
    SparkEnv.get.conf.get(LocalOpts.alwaysUseRemote.key).toBoolean

  override def makeInputStream(): InputStream = {
    if (!useRemote && isLocal) {
      logInfo(s"found $blockId locally,  do local read.")
      new FileInputStream(file)
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

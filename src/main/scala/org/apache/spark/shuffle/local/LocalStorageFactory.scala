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

import java.io.File
import java.util
import java.util.Collections

import com.memverge.splash.{ShuffleFile, ShuffleListener, StorageFactory, TmpShuffleFile}
import org.apache.commons.io.FileUtils
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

class LocalStorageFactory extends StorageFactory with Logging {

  override def makeSpillFile(): TmpShuffleFile = LocalTmpShuffleFile.make()

  override def makeDataFile(path: String): TmpShuffleFile =
    LocalTmpShuffleFile.make(getDataFile(path))

  override def makeIndexFile(path: String): TmpShuffleFile =
    LocalTmpShuffleFile.make(getIndexFile(path))

  override def getDataFile(path: String): ShuffleFile =
    new LocalShuffleFile(path)

  override def getIndexFile(path: String): ShuffleFile =
    new LocalShuffleFile(path)

  override def getListeners: util.Collection[ShuffleListener] =
    Collections.emptyList()

  override def getShuffleFolder(appId: String): String =
    LocalShuffleUtil.getShuffleFolder(appId)

  override def cleanShuffle(appId: String): Unit = {
    val folderName = getShuffleFolder(appId)
    val folder = new File(folderName)
    if (folderName.contains(appId) && folder.exists()) {
      FileUtils.deleteQuietly(folder)
    }
  }

  override def getShuffleFileCount(appId: String): Int = {
    val folder = new File(getShuffleFolder(appId))
    FileUtils.listFiles(folder, Array("data", "index"), true).size()
  }

  override def getTmpFileCount: Int = {
    val folder = new File(LocalShuffleUtil.getTmpFolder())
    FileUtils.listFiles(folder, null, true)
        .iterator.asScala
        .count(file => file.getName.contains("tmp-"))
  }

  override def reset(): Unit = {
    val blockManagerFolder = LocalShuffleUtil.diskBlockManagerFolder
    if (!blockManagerFolder.contains("blockmgr")) {
      FileUtils.deleteDirectory(new File(blockManagerFolder))
      logInfo(s"Reset storage, remove $blockManagerFolder.")
    }

    val tmpFolder = LocalShuffleUtil.getTmpFolder()
    FileUtils.deleteDirectory(new File(tmpFolder))
    logInfo(s"Reset storage, remove $tmpFolder.")
  }
}

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

import java.io._
import java.nio.file.Paths
import java.util.UUID

import com.memverge.splash.{ShuffleFile, TempFolder, TmpShuffleFile}
import org.apache.spark.internal.Logging

class LocalTmpShuffleFile extends TmpShuffleFile with Logging {

  private var _uuid: UUID = _
  private var commitTarget: LocalShuffleFile = _

  private def file: File = new File(LocalTmpShuffleFile.uuidToPath(uuid()))

  override def swap(other: TmpShuffleFile): Unit = {
    if (!other.exists) {
      val message = "Can only swap with a uncommitted tmp file"
      throw new IOException(message)
    }

    val otherLocal = other.asInstanceOf[LocalTmpShuffleFile]

    delete()

    val tmpUuid = otherLocal.uuid()
    otherLocal.setUuid(uuid())
    setUuid(tmpUuid)
  }

  private def setUuid(id: UUID): Unit = _uuid = id

  override def makeOutputStream(): OutputStream = {
    try {
      create()
      logDebug(s"create output stream for $getPath")
      new FileOutputStream(file, false)
    } catch {
      case e: FileNotFoundException =>
        throw new IllegalArgumentException(s"Create OS failed for $getPath.", e)
    }
  }

  override def getCommitTarget: ShuffleFile = commitTarget

  private[spark] def rename(tgtId: String): Unit = {
    val tgtFile = new File(tgtId)
    val tgtParent = tgtFile.getParentFile
    if (!tgtParent.exists()) {
      if (!tgtParent.mkdirs()) {
        logInfo(s"failed to create parent ${tgtParent.getAbsolutePath}")
      }
    }

    logDebug(s"rename $getPath to $tgtId.")
    LocalShuffleUtil.putBlock(file, tgtId)
  }

  override def commit(): ShuffleFile = {
    if (commitTarget == null) {
      throw new IOException("No commit target.")
    } else if (!exists) {
      create()
    }
    if (commitTarget.exists()) {
      logWarning(s"commit target already exists, remove '${commitTarget.getPath}'.")
      commitTarget.file.delete()
    }
    logDebug(s"commit tmp file $getPath to target file ${getCommitTarget.getPath}.")

    rename(commitTarget.getPath)
    commitTarget
  }

  override def recall(): Unit = {
    val commitTarget = getCommitTarget
    if (commitTarget != null) {
      logInfo(s"recall tmp file $getPath of target file ${commitTarget.getPath}.")
    } else {
      logInfo(s"recall tmp file $getPath without target file.")
    }
    delete()
  }

  override def uuid(): UUID = _uuid

  override def create(): TmpShuffleFile = {
    val parent = file.getParentFile
    if (!parent.exists()) {
      logInfo(s"create folder ${parent.getAbsolutePath}")
      parent.mkdirs()
    }
    logDebug(s"create file ${file.getAbsolutePath}")
    if (!file.createNewFile()) {
      throw new IOException(s"file $getPath already exists.")
    } else {
      logDebug(s"file $getPath created")
    }
    this
  }

  override def getSize: Long = file.length()

  override def delete(): Boolean = file.delete()

  override def exists(): Boolean = file.exists()

  override def getPath: String = file.getAbsolutePath

  override def makeInputStream(): InputStream = {
    try {
      log.debug("create input stream for {}.", getPath)
      new FileInputStream(file)
    } catch {
      case e: FileNotFoundException =>
        throw new IllegalArgumentException(s"Create IS failed for $getPath.", e)
    }
  }
}

private[spark] object LocalTmpShuffleFile {
  private val folder = TempFolder.getInstance()
  private val prefix = "tmp-"

  private def uuidToPath(uuid: UUID): String = {
    val tmpPath = folder.getTmpPath
    val filename = s"$prefix${uuid.toString}"
    Paths.get(tmpPath, filename).toString
  }

  def make(): LocalTmpShuffleFile = {
    val ret = new LocalTmpShuffleFile()
    ret.setUuid(UUID.randomUUID())
    ret
  }

  def make(file: ShuffleFile): LocalTmpShuffleFile = {
    require(file != null, "file should not be null")
    val ret = make()
    ret.commitTarget = file.asInstanceOf[LocalShuffleFile]
    ret
  }
}

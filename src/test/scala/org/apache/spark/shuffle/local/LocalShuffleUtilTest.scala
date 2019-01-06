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

import org.apache.spark.storage.{ShuffleDataBlockId, ShuffleIndexBlockId, TempLocalBlockId}
import org.assertj.core.api.Assertions.{assertThat, assertThatExceptionOfType}
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.testng.annotations.Test

@Test(groups = Array("UnitTest", "IntegrationTest"))
class LocalShuffleUtilTest {
  def testToShuffleDataId(): Unit = {
    val filename = "/a/b/c/shuffle_51_21_95.data"
    val blockId = LocalShuffleUtil.toBlockId(filename)
    val dataBlockId = blockId.asInstanceOf[ShuffleDataBlockId]
    assertThat(dataBlockId.shuffleId) isEqualTo 51
    assertThat(dataBlockId.mapId) isEqualTo 21
    assertThat(dataBlockId.reduceId) isEqualTo 95
  }

  def testToShuffleIndexId(): Unit = {
    val filename = "/a/b/c/shuffle_52_23_91.index"
    val blockId = LocalShuffleUtil.toBlockId(filename)
    val dataBlockId = blockId.asInstanceOf[ShuffleIndexBlockId]
    assertThat(dataBlockId.shuffleId) isEqualTo 52
    assertThat(dataBlockId.mapId) isEqualTo 23
    assertThat(dataBlockId.reduceId) isEqualTo 91
  }

  def testToTmpLocalBlockId(): Unit = {
    val filename = "\\splash\\tmp\\tmp-3ac0bb9b-0c33-4ec3-91f8-cfee8d786b5f"
    val blockId = LocalShuffleUtil.toBlockId(filename)
    val tmpBlockId = blockId.asInstanceOf[TempLocalBlockId]
    assertThat(tmpBlockId.toString())
        .isEqualTo("temp_local_3ac0bb9b-0c33-4ec3-91f8-cfee8d786b5f")
  }

  def testUnrecognizedBlockId(): Unit = {
    val filename = "/a/b/c/shuffle_23_53.data"
    assertThatExceptionOfType(classOf[IllegalArgumentException])
        .isThrownBy(new ThrowingCallable {
          override def call(): Unit = LocalShuffleUtil.toBlockId(filename)
        })
  }

  def testGetShuffleFolderIfDiskManagerNotExists(): Unit = {
    val appId = "app-123"
    val folder = LocalShuffleUtil.getShuffleFolder(appId)
    assertThat(folder).contains("splash", appId, "shuffle")
  }

  def testGetTempFolderIfDiskManagerNotExists(): Unit = {
    val folder = LocalShuffleUtil.getTmpFolder
    assertThat(folder).contains("splash", "tmp")
  }
}

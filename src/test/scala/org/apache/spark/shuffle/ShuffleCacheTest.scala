/*
 * Modifications copyright (C) 2019 MemVerge Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.shuffle

import com.memverge.splash.StorageFactoryHolder
import org.apache.spark.storage.ShuffleBlockId
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.{AfterMethod, BeforeMethod, Test}


@Test(groups = Array("UnitTest", "IntegrationTest"))
class ShuffleCacheTest {
  private val shuffleId = 2
  private val resolver = new SplashShuffleBlockResolver("shuffleCacheTest")
  private var shuffleCache: ShuffleCache = _

  @BeforeMethod
  private def beforeMethod(): Unit = shuffleCache = ShuffleCache(resolver)

  @AfterMethod
  private def afterMethod(): Unit = {
    shuffleCache.invalidateAll()
    StorageFactoryHolder.getFactory.reset()
  }

  private def writeTestData(shuffleId: Int, mapId: Int): Unit = {
    val length = Array(10L, 0L, 20L)
    val dataTmp = resolver.getDataTmpFile(shuffleId, mapId)
    resolver.writeData(dataTmp, (1 to 30).map(_.toByte).toArray)
    resolver.writeIndexFileAndCommit(shuffleId, mapId, length, dataTmp)
  }

  def testCacheIndexFile(): Unit = {
    val mapId = 0
    writeTestData(shuffleId, mapId)

    val indexFile = resolver.getIndexFile(shuffleId, mapId)
    assertThat(indexFile.exists()) isTrue()

    val blockId = ShuffleBlockId(shuffleId, mapId, 2)
    val expected = Some(PartitionLoc(10, 30))
    assertThat(shuffleCache.getPartitionLoc(blockId)).isEqualTo(expected)

    indexFile.delete()
    assertThat(indexFile.exists()) isFalse()

    // make sure we could still read the location from cache
    assertThat(shuffleCache.getPartitionLoc(blockId)).isEqualTo(expected)
  }

  def testInvalidateSpecifiedShuffle(): Unit = {
    val mapId = 1
    writeTestData(shuffleId, mapId)
    writeTestData(shuffleId + 1, mapId)
    val expected = Some(PartitionLoc(10, 30))

    val blockId = ShuffleBlockId(shuffleId, mapId, 2)
    val blockId1 = ShuffleBlockId(shuffleId + 1, mapId, 2)
    // make sure we cache the value
    assertThat(shuffleCache.getPartitionLoc(blockId)).isEqualTo(expected)
    assertThat(shuffleCache.getPartitionLoc(blockId1)).isEqualTo(expected)

    resolver.getIndexFile(shuffleId, mapId).delete()
    resolver.getIndexFile(shuffleId + 1, mapId).delete()
    shuffleCache.invalidateCache(shuffleId)

    assertThat(shuffleCache.getPartitionLoc(blockId)).isEqualTo(None)
    assertThat(shuffleCache.getPartitionLoc(blockId1)).isEqualTo(expected)
  }

  def testInvalidateAllCache(): Unit = {
    val mapId = 2
    writeTestData(shuffleId, mapId)
    val expected = Some(PartitionLoc(10, 30))

    val blockId = ShuffleBlockId(shuffleId, mapId, 2)
    // make sure we cache the value
    assertThat(shuffleCache.getPartitionLoc(blockId)).isEqualTo(expected)
    resolver.getIndexFile(shuffleId, mapId).delete()
    shuffleCache.invalidateAll()

    assertThat(shuffleCache.getPartitionLoc(blockId)).isEqualTo(None)
  }

  def testCacheDataFileReference(): Unit = {
    val mapId = 3
    writeTestData(shuffleId, mapId)
    val blockId = ShuffleBlockId(shuffleId, mapId, 1)

    val data0 = shuffleCache.getDataFile(blockId)
    val data1 = shuffleCache.getDataFile(blockId)
    assertThat(data0).isEqualTo(data1)

    shuffleCache.invalidateCache(shuffleId)
    val data2 = shuffleCache.getDataFile(blockId)
    assertThat(data0).isNotEqualTo(data2)
  }

  def testInvalidPartitionNumber(): Unit = {
    val mapId = 4
    writeTestData(shuffleId, mapId)
    val blockId = ShuffleBlockId(shuffleId, mapId, 100)
    assertThat(shuffleCache.getPartitionLoc(blockId)).isEqualTo(None)
  }
}

/*
 * Modifications copyright (C) 2018 MemVerge Inc.
 *
 * Replace the original shuffle class with Splash version classes.
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
import org.apache.spark.SparkContext
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations._


@Test(groups = Array("UnitTest", "IntegrationTest"))
class SplashShuffleBlockResolverTest {
  private var resolver: SplashShuffleBlockResolver = _
  private var sc: SparkContext = _

  @BeforeClass
  def beforeClass(): Unit = {
    sc = TestUtil.newSparkContext(TestUtil.newSparkConf())
  }

  @AfterClass
  def afterClass(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

  @BeforeMethod
  def beforeMethod(): Unit = {
    resolver = new SplashShuffleBlockResolver("test-app")
  }

  @AfterMethod
  def afterMethod(): Unit = {
    StorageFactoryHolder.getFactory.reset()
  }

  private val shuffleId = 1

  def testCommitShuffleDataFile(): Unit = {
    val lengths = Array(10L, 0L, 20L)
    val mapId = 1
    val dataTmp = resolver.getDataTmpFile(shuffleId, mapId)

    resolver.writeData(dataTmp, new Array[Byte](30))
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)

    val dataFile = resolver.getDataFile(shuffleId, mapId)
    assertThat(dataFile.exists()) isTrue()
    assertThat(dataFile.getSize) isEqualTo 30
    assertThat(dataTmp.exists()) isFalse()
  }

  def testCommitShuffleDataMultipleTimes(): Unit = {
    val mapId = 2

    // first write, with valid data
    val lengths1 = Array(10L, 0L, 20L)
    val dataTmp1 = resolver.getDataTmpFile(shuffleId, mapId)

    resolver.writeData(dataTmp1, new Array[Byte](30))
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths1, dataTmp1)

    // second write, ignore this because data already exists
    val dataTmp2 = resolver.getDataTmpFile(shuffleId, mapId)
    val lengths2 = new Array[Long](3)

    resolver.writeData(dataTmp2, Array[Byte](1) ++ new Array[Byte](29))
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths2, dataTmp2)

    var dataFile = resolver.getDataFile(shuffleId, mapId)
    assertThat(lengths1.toSeq) isEqualTo lengths2.toSeq
    assertThat(dataFile.exists()) isTrue()
    assertThat(dataFile.getSize) isEqualTo 30
    assertThat(dataTmp2.exists()) isFalse()

    // remove data file and third write, write should success
    dataFile.delete()
    val lengths3 = Array[Long](10, 10, 15)
    val dataTmp3 = resolver.getDataTmpFile(shuffleId, mapId)

    resolver.writeData(dataTmp3, Array[Byte](2) ++ new Array[Byte](34))
    dataFile = resolver.getDataFile(shuffleId, mapId)
    resolver.writeIndexFileAndCommit(1, 2, lengths3, dataTmp3)

    assertThat(lengths1.toSeq) isNotEqualTo lengths3.toSeq
    assertThat(dataFile.exists()) isTrue()
    assertThat(dataFile.getSize) isEqualTo 35
    assertThat(dataTmp3.exists()) isFalse()

    SplashUtils.withResources(dataFile.makeInputStream()) { is =>
      assertThat(is.read()) isEqualTo 2
    }
  }

  def testRemoveDataByMap(): Unit = {
    val mapId = 3
    val dataFile = resolver.getDataTmpFile(shuffleId, mapId).create().commit()
    val indexFile = resolver.getIndexTmpFile(shuffleId, mapId).create().commit()

    assertThat(dataFile.exists()) isTrue()
    assertThat(indexFile.exists()) isTrue()

    resolver.removeDataByMap(shuffleId, mapId)
    assertThat(dataFile.exists()) isFalse()
    assertThat(indexFile.exists()) isFalse()
  }

  def testCheckIndexAndDataFile(): Unit = {
    val indices = Array(10L, 0L, 20L)
    val mapId = 4

    resolver.writeShuffle(shuffleId, mapId, indices, new Array[Byte](30))

    val actual = resolver.checkIndexAndDataFile(shuffleId, mapId)
    assertThat(actual) isEqualTo indices
  }

  def testCheckIndexAndDataFileSizeNotMatch(): Unit = {
    val indices = Array(10L, 0L, 20L)
    val mapId = 5
    val tmpIndexFile = resolver.getIndexTmpFile(shuffleId, mapId)
    resolver.writeIndices(tmpIndexFile, indices)
    tmpIndexFile.commit()
    val tmpDataFile = resolver.getDataTmpFile(shuffleId, mapId)
    resolver.writeData(tmpDataFile, new Array[Byte](29))
    tmpDataFile.commit()

    val actual = resolver.checkIndexAndDataFile(shuffleId, mapId)
    assertThat(actual) isNull()
  }

  def testCommitEmptyShuffleIndex(): Unit = {
    val lengths = Array[Long]()
    val mapId = 6
    val dataTmp = resolver.getDataTmpFile(shuffleId, mapId)

    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)
    val dataFile = resolver.getDataFile(shuffleId, mapId)
    val indexFile = resolver.getIndexFile(shuffleId, mapId)
    assertThat(dataFile.exists()) isTrue()
    assertThat(dataFile.getSize) isEqualTo 0
    assertThat(dataTmp.exists()) isFalse()
    assertThat(indexFile.exists()) isTrue()
    assertThat(indexFile.getSize) isEqualTo 0
  }

  def testCheckIndexAndDataFileEmptyIndex(): Unit = {
    val indices = Array[Long]()
    val mapId = 7
    val tmpIndexFile = resolver.getIndexTmpFile(shuffleId, mapId)
    resolver.writeIndices(tmpIndexFile, indices)
    tmpIndexFile.commit()
    resolver.getDataTmpFile(shuffleId, mapId).commit()

    val actual = resolver.checkIndexAndDataFile(shuffleId, mapId)
    assertThat(actual) isEqualTo indices
  }

  def testCheckIndexAndDataFileZeroLengthIndex(): Unit = {
    val mapId = 8
    resolver.getIndexTmpFile(shuffleId, mapId).commit()
    resolver.getDataTmpFile(shuffleId, mapId).commit()

    val actual = resolver.checkIndexAndDataFile(shuffleId, mapId)
    assertThat(actual) isNull()
  }

  def testCheckIndexAndDataFileEmptyPartitions(): Unit = {
    val indices = Array[Long](0, 0, 0, 0)
    val mapId = 9
    val tmpIndexFile = resolver.getIndexTmpFile(shuffleId, mapId)
    resolver.writeIndices(tmpIndexFile, indices)
    tmpIndexFile.commit()
    resolver.getDataTmpFile(shuffleId, mapId).commit()

    val actual = resolver.checkIndexAndDataFile(shuffleId, mapId)
    assertThat(actual) isEqualTo indices
  }
}

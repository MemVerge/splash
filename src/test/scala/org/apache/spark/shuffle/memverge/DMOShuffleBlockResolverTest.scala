/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle.memverge

import com.memverge.mvfs.connection.MVFSConnectOptions
import com.memverge.mvfs.dmo.{DMOFile, DMOInputStream}
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.{AfterMethod, BeforeMethod, Test}


@Test(groups = Array("UnitTest", "IntegrationTest"))
class DMOShuffleBlockResolverTest {
  private var resolver: DMOShuffleBlockResolver = _

  @BeforeMethod
  def beforeMethod(): Unit = {
    resolver = new DMOShuffleBlockResolver("test-app",
      indexChunkSizeKB = 2,
      dataChunkSizeKB = 20)
  }

  @AfterMethod
  def afterMethod(): Unit = {
    DMOFile.safeReset()
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
    dataFile.forceDelete()
    val lengths3 = Array[Long](10, 10, 15)
    val dataTmp3 = resolver.getDataTmpFile(shuffleId, mapId)

    resolver.writeData(dataTmp3, Array[Byte](2) ++ new Array[Byte](34))
    dataFile = resolver.getDataFile(shuffleId, mapId)
    resolver.writeIndexFileAndCommit(1, 2, lengths3, dataTmp3)

    assertThat(lengths1.toSeq) isNotEqualTo lengths3.toSeq
    assertThat(dataFile.exists()) isTrue()
    assertThat(dataFile.getSize) isEqualTo 35
    assertThat(dataTmp3.exists()) isFalse()

    DMOUtils.withResources(new DMOInputStream(dataFile)) { is =>
      assertThat(is.read()) isEqualTo 2
    }
  }

  def testRemoveDataByMap(): Unit = {
    val mapId = 3
    val dataFile = resolver.getDataFile(shuffleId, mapId).create()
    val indexFile = resolver.getIndexFile(shuffleId, mapId).create()

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
    resolver.writeIndices(resolver.getIndexFile(shuffleId, mapId), indices)
    resolver.writeData(resolver.getDataFile(shuffleId, mapId), new Array[Byte](29))

    val actual = resolver.checkIndexAndDataFile(shuffleId, mapId)
    assertThat(actual) isNull()
  }

  def testIndexFileChunkSize(): Unit = {
    val indexTmpFile = resolver.getIndexTmpFile(0, 0)
    assertThat(indexTmpFile.getChunkSize) isEqualTo 2 * 1024

    val indexFile = resolver.getIndexFile(0, 1)
    assertThat(indexFile.getChunkSize) isEqualTo 2 * 1024
  }

  def testDataFileChunkSize(): Unit = {
    val dataTmpFile = resolver.getDataTmpFile(0, 0)
    assertThat(dataTmpFile.getChunkSize) isEqualTo 20 * 1024

    val dataFile = resolver.getDataFile(0, 1)
    assertThat(dataFile.getChunkSize) isEqualTo 20 * 1024

    val tmpFile = resolver.getDataTmpFile(MVFSConnectOptions.defaultSocket)
    assertThat(tmpFile.getChunkSize) isEqualTo 20 * 1024
  }
}

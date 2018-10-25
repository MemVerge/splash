/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.storage.memverge

import java.nio.ByteBuffer

import com.memverge.mvfs.dmo.DMOFile
import org.apache.commons.io.IOUtils
import org.apache.spark.shuffle.memverge.TestUtil
import org.apache.spark.storage.BlockId
import org.apache.spark.util.io.ChunkedByteBuffer
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.{AfterClass, Test}

@Test(groups = Array("UnitTest", "IntegrationTest"))
class DMOStoreTest {
  private val conf = TestUtil.newSparkConf()
  private val dmoStore = new DMOStore(conf, "testBlockManager")

  @AfterClass
  def afterClass(): Unit = {
    DMOFile.safeReset()
  }

  private def putBlock(blockId: BlockId): Array[Byte] = {
    // Create a non-trivial (not all zeros) byte array
    val bytes = Array.tabulate[Byte](1000)(_.toByte)
    val byteBuffer = new ChunkedByteBuffer(ByteBuffer.wrap(bytes))
    dmoStore.putBytes(blockId, byteBuffer)
    bytes
  }

  def testReadFiles(): Unit = {
    val blockId = BlockId("rdd_1_2")
    val bytes = putBlock(blockId)

    val fromDmo = dmoStore.getBytes(blockId)
    assertThat(dmoStore.remove(blockId)).isTrue
    assertThat(fromDmo.toArray) isEqualTo bytes
  }

  def testInputStream(): Unit = {
    val blockId = BlockId("rdd_1_1")
    val bytes = putBlock(blockId)

    val fromDmo = dmoStore.getInputStream(blockId)
    val buffer = new Array[Byte](1000)
    IOUtils.read(fromDmo, buffer)
    
    assertThat(buffer) isEqualTo bytes
    assertThat(dmoStore.remove(blockId)).isTrue
  }
}

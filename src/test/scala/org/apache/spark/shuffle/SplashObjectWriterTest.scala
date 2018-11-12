/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle

import com.memverge.splash.{StorageFactoryHolder, TmpShuffleFile}
import org.apache.spark.storage.ShuffleDataBlockId
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.{AfterClass, Test}

@Test(groups = Array("UnitTest", "IntegrationTest"))
class SplashObjectWriterTest {
  private var tmpFile: TmpShuffleFile = _
  private var objWriter: SplashObjectWriter = _
  private val blockId = ShuffleDataBlockId(1, 2, 3)
  private val storageFactory = StorageFactoryHolder.getFactory

  @AfterClass
  def afterClass(): Unit = {
    StorageFactoryHolder.getFactory.reset()
  }

  def testCommitWithoutInitialize(): Unit = {
    tmpFile = storageFactory.makeSpillFile()
    objWriter = new SplashObjectWriter(blockId, tmpFile)

    assertThat(objWriter.commitAndGet()) isEqualTo 0
  }
}

/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle.memverge

import com.memverge.mvfs.dmo.{DMOFile, DMOTmpFile}
import org.apache.spark.storage.ShuffleDataBlockId
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.{AfterClass, Test}

@Test(groups = Array("UnitTest", "IntegrationTest"))
class DMOObjectWriterTest {
  private var tmpFile: DMOTmpFile = _
  private var objWriter: DMOObjectWriter = _
  private val blockId = ShuffleDataBlockId(1, 2, 3)

  @AfterClass
  def afterClass(): Unit = {
    DMOFile.safeReset()
  }

  def testCommitWithoutInitialize(): Unit = {
    tmpFile = DMOTmpFile.make()
    objWriter = new DMOObjectWriter(blockId, tmpFile)

    assertThat(objWriter.commitAndGet()) isEqualTo 0
  }
}

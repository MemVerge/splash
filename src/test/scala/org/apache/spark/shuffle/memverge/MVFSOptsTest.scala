/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle.memverge

import com.memverge.mvfs.connection.MVFSConnectOptions
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.Test

@Test(groups = Array("UnitTest", "IntegrationTest"))
class MVFSOptsTest {
  private val conf = TestUtil.newSparkConf()

  def testDefaultValues(): Unit = {
    assertThat(conf.get(MVFSOpts.mvfsSocket)) isEqualTo MVFSConnectOptions.defaultSocket()
    assertThat(conf.get(MVFSOpts.mvfsHandleCacheSize)) isEqualTo -1
    assertThat(conf.get(MVFSOpts.mvfsDataChunkSizeKB)) isEqualTo 1024
    assertThat(conf.get(MVFSOpts.mvfsIndexChunkSize)) isEqualTo 1
    assertThat(conf.get(MVFSOpts.mvfsShuffleFileBufferKB)) isEqualTo 32
    assertThat(conf.get(MVFSOpts.shuffleInitialBufferSize)) isEqualTo 4096
  }
}

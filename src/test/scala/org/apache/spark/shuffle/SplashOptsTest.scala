/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle

import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.Test

@Test(groups = Array("UnitTest", "IntegrationTest"))
class SplashOptsTest {
  private val conf = TestUtil.newSparkConf()

  def testDefaultValues(): Unit = {
    assertThat(conf.get(SplashOpts.shuffleFileBufferKB)) isEqualTo 32
    assertThat(conf.get(SplashOpts.shuffleInitialBufferSize)) isEqualTo 4096
  }

  def testSetValue(): Unit = {
    val option = SplashOpts.fastMergeEnabled
    conf.set(option, false)
    assertThat(conf.get(option)).isFalse
  }
}

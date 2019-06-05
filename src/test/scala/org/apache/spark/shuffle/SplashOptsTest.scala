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
package org.apache.spark.shuffle

import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.Test

@Test(groups = Array("UnitTest", "IntegrationTest"))
class SplashOptsTest {
  private val conf = TestUtil.newSparkConf()

  def testDefaultValues(): Unit = {
    assertThat(conf.get(SplashOpts.shuffleFileBufferKB)) isEqualTo 32L
    assertThat(conf.get(SplashOpts.shuffleInitialBufferSize)) isEqualTo 4096
    assertThat(conf.get(SplashOpts.storageFactoryName)) isNotEmpty()
    assertThat(conf.get(SplashOpts.memoryMapThreshold)) isGreaterThan 0L
    assertThat(conf.get(SplashOpts.localSplashFolder)) isNull()
    assertThat(conf.get(SplashOpts.clearShuffleOutput)) isTrue()
    assertThat(conf.get(SplashOpts.useRadixSort)) isTrue()
    assertThat(conf.get(SplashOpts.shuffleCompress)) isTrue()
  }

  def testConfigsNotExistsInSpark210(): Unit = {
    assertThat(conf.get(SplashOpts.shuffleFileBufferKB)) isEqualTo 32L
    assertThat(conf.get(SplashOpts.forceSpillElements)) isEqualTo Integer.MAX_VALUE
  }

  def testSetValue(): Unit = {
    val option = SplashOpts.fastMergeEnabled
    conf.set(option, false)
    assertThat(conf.get(option)).isFalse
  }
}

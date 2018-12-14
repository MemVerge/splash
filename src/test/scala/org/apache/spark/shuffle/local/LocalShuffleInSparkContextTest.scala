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

import org.apache.spark.SparkContext
import org.apache.spark.shuffle.TestUtil
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.{AfterClass, BeforeClass, Test}

@Test(groups = Array("UnitTest", "IntegrationTest"))
class LocalShuffleInSparkContextTest {
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

  def testGetLocalShuffleFolder(): Unit = {
    val appId = "app-123"
    val folder = LocalShuffleUtil.getShuffleFolder(appId)
    assertThat(folder).contains("blockmgr")
    assertThat(folder).doesNotContain("shuffle", appId, "splash")
  }

  def testGetTmpFolder(): Unit = {
    val folder = LocalShuffleUtil.getTmpFolder()
    assertThat(folder).contains("tmp", "splash")
  }
}

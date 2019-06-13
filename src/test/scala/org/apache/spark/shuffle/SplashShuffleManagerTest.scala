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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Fail.fail
import org.testng.annotations.Test

@Test(groups = Array("UnitTest"))
class SplashShuffleManagerTest {
  def testVersion(): Unit = {
    val version = SplashShuffleManager.version
    assertThat(version).matches("\\d+\\.\\d+\\.\\d+")
  }

  def testStopWithoutApp(): Unit = {
    val shuffleManager = new SplashShuffleManager(TestUtil.confWithKryo)
    try {
      shuffleManager.stop()
    } catch {
      case e: Exception =>
        fail(s"should not raise exception", e)
    }
  }
}

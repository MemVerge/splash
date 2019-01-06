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

import com.memverge.splash.{StorageFactoryHolder, TmpShuffleFile}
import org.apache.spark.storage.ShuffleDataBlockId
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.{AfterClass, AfterMethod, Test}

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

  @AfterMethod
  def afterMethod(): Unit = {
    if (objWriter != null) {
      objWriter.close()
    }
  }

  def testCommitWithoutInitialize(): Unit = {
    tmpFile = storageFactory.makeSpillFile()
    objWriter = new SplashObjectWriter(blockId, tmpFile)

    assertThat(objWriter.commitAndGet()) isEqualTo 0
  }
}

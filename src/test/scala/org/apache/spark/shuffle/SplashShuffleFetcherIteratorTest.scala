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

import com.memverge.splash.StorageFactoryHolder
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.{ShuffleBlockId, TestBlockId}
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Fail.fail
import org.testng.annotations.{AfterMethod, BeforeMethod, Test}


@Test(groups = Array("UnitTest", "IntegrationTest"))
class SplashShuffleFetcherIteratorTest {
  private val appId = "SplashShuffleFetcherIteratorTest"
  private val factory = StorageFactoryHolder.getFactory
  private var resolver: SplashShuffleBlockResolver = _

  @BeforeMethod
  private def beforeMethod(): Unit = {
    resolver = new SplashShuffleBlockResolver(appId)
  }

  @AfterMethod
  private def afterMethod(): Unit = {
    factory.reset()
    assertThat(factory.getShuffleFileCount(appId)) isEqualTo 0
    assertThat(factory.getTmpFileCount) isEqualTo 0
  }

  def testNext(): Unit = {
    val blocks = List(
      resolver.putShuffleBlock(2, 1, Array(10L, 20L, 30L)),
      resolver.putShuffleBlock(2, 2, Array(30L, 15L, 22L)))
    val fetchers = SplashShuffleFetcherIterator(resolver, blocks.iterator)
    assertThat(fetchers.hasNext).isTrue
    val fetcher1 = fetchers.next()
    assertThat(fetcher1.blockId) isEqualTo ShuffleBlockId(2, 1, 0)
    assertThat(fetcher1.length) isEqualTo 10
    fetcher1.close()

    val fetcher2 = fetchers.next()
    assertThat(fetcher2.blockId) isEqualTo ShuffleBlockId(2, 2, 0)
    assertThat(fetcher2.length) isEqualTo 30
    fetcher2.close()
  }

  def testDumpOnError(): Unit = {
    val serializer = TestUtil.kryoSerializer
    val blocks = List(
      resolver.putShuffleBlock(3, 1, Array(10L, 20L, 30L)),
      resolver.putShuffleBlock(3, 2, Array(30L, 15L, 22L)))
    val fetchers = SplashShuffleFetcherIterator(resolver, blocks.iterator)
    val iterator = fetchers.flatMap(
      fetcher => fetcher.asMetricIterator(serializer, TaskMetrics.empty))
    try {
      iterator.next()
      fail("should have raised an exception.")
    } catch {
      case _: Exception =>
        val path = resolver.getDumpFilePath(ShuffleBlockId(3, 2, 0))
        assertThat(path.toFile.exists()).isTrue
    }
  }

  def testNoNextValue(): Unit = {
    val blocks = List(TestBlockId("block-1"))
    val fetchers = SplashShuffleFetcherIterator(resolver, blocks.iterator)
    assertThat(fetchers.hasNext).isFalse
  }

  def testSkipNonShuffleBlocks(): Unit = {
    val blocks = List(
      TestBlockId("block-1"),
      TestBlockId("block-2"),
      resolver.putShuffleBlock(4, 2, Array(30L, 15L, 22L)))
    val fetchers = SplashShuffleFetcherIterator(resolver, blocks.iterator).toArray
    assertThat(fetchers.length) isEqualTo 1
    fetchers.foreach(_.close())
  }
}

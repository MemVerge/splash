/*
 * Modifications copyright (C) 2018 MemVerge Corp
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
package org.apache.spark.shuffle.sort

import com.memverge.splash.StorageFactoryHolder
import org.apache.spark.shuffle.{ConfWithReducerN, SplashOpts, TestUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.{AfterMethod, DataProvider, Test}

@Test(groups = Array("UnitTest", "IntegrationTest"))
class SplashUnsafeSorterTest {
  private var sc: SparkContext = _

  @AfterMethod
  def afterMethod(): Unit = {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    val storageFactory = StorageFactoryHolder.getFactory
    storageFactory.reset()
    assertThat(storageFactory.getTmpFileCount).isEqualTo(0)
  }

  private def newSparkContextWithForceSpillSize(
      conf: SparkConf, size: Int): SparkContext = {
    conf.set(SplashOpts.forceSpillElements, size)
    TestUtil.newSparkContext(conf)
  }

  private def sparkConfWithDifferentSer: Array[SparkConf] = {
    TestUtil.getSparkConfArray
  }

  private def confWithReducerNumList: Array[ConfWithReducerN] = {
    sparkConfWithDifferentSer.flatMap { conf =>
      List(ConfWithReducerN(conf, 2), ConfWithReducerN(conf, 100))
    }
  }

  @DataProvider(name = "confWithDiffFastMerge")
  def confWithDiffFastMerge: Array[ConfWithReducerN] = {
    val key = SplashOpts.fastMergeEnabled
    confWithReducerNumList.flatMap { case ConfWithReducerN(conf, numReducer) =>
      List(ConfWithReducerN(conf.clone.set(key, false), numReducer),
        ConfWithReducerN(conf.clone.set(key, true), numReducer)
      )
    }
  }

  @Test(dataProvider = "confWithDiffFastMerge")
  def testSpillLocalSortByKey(conf: ConfWithReducerN): Unit = {
    val size = 1000
    sc = newSparkContextWithForceSpillSize(conf.conf, size / 4)
    TestUtil.assertSpilled(sc) {
      val result = sc.parallelize(0 until size)
          .map { i => (i / 2, i) }
          .sortByKey(numPartitions = conf.numReducer)
          .collect()
      val expected = (0 until size).map { i => (i / 2, i) }.toArray
      assertThat(result) hasSize size
      result.zipWithIndex.foreach { case ((k, _), i) =>
        val (expectedKey, _) = expected(i)
        assertThat(k) isEqualTo expectedKey
      }
    }
  }

  @Test(dataProvider = "confWithDiffFastMerge")
  def testSpillLocalCoGroup(conf: ConfWithReducerN): Unit = {
    val size = 1000
    sc = newSparkContextWithForceSpillSize(conf.conf, size / 4)
    TestUtil.assertSpilled(sc) {
      val rdd1 = sc.parallelize(0 until size).map { i => (i / 2, i) }
      val rdd2 = sc.parallelize(0 until size).map { i => (i / 2, i) }
      val result = rdd1.cogroup(rdd2, conf.numReducer).collect()
      assertThat(result.length) isEqualTo size / 2
      result.foreach { case (i, (seq1, seq2)) =>
        val actual1 = seq1.toSet
        val actual2 = seq2.toSet
        val expected = Set(i * 2, i * 2 + 1)
        assertThat(actual1) isEqualTo expected
        assertThat(actual2) isEqualTo expected
      }
    }
  }

  def testSortUpdatesPeakExecutionMemory(): Unit = {
    val size = 1000
    sc = newSparkContextWithForceSpillSize(TestUtil.confWithoutKryo, size)
    TestUtil.verifyPeakExecutionMemorySet(sc, "external sorter without spilling") {
      TestUtil.assertNotSpilled(sc) {
        sc.parallelize(1 to size / 2, 2).repartition(100).count()
      }
    }
    TestUtil.verifyPeakExecutionMemorySet(sc, "external sorter with spilling") {
      TestUtil.assertSpilled(sc) {
        sc.parallelize(1 to size * 3, 2).repartition(100).count()
      }
    }
  }
}

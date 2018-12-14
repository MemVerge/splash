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
import org.apache.spark.scheduler.MapStatus
import org.apache.spark._
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.{AfterClass, BeforeClass, BeforeMethod, Test}

@Test(groups = Array("UnitTest", "IntegrationTest"))
class SplashBypassMergeSortShuffleWriterTest {
  private val appId = "test-bypass-merge-sort-shuffle-writer-app"
  private val resolver = new SplashShuffleBlockResolver(appId)
  private val reducerNum = 7
  private val mapId = 1
  private val storageFactory = StorageFactoryHolder.getFactory

  private var sc: SparkContext = _
  private var shuffleId = 0
  private var writer: SplashBypassMergeSortShuffleWriter[Int, Int] = _
  private var taskContext: TaskContext = _

  private def dataFile = resolver.getDataFile(shuffleId, mapId)

  @BeforeClass
  def beforeClass(): Unit = {
    val conf = TestUtil.newSparkConf(4)
        .setAppName(appId)
        .set(SplashOpts.bypassSortThreshold, 200)
    sc = TestUtil.newSparkContext(conf)
  }

  @AfterClass
  def afterClass(): Unit = {
    sc.stop()
    storageFactory.reset()
  }

  @BeforeMethod
  def beforeMethod(): Unit = {
    val rdd = sc.parallelize((1 to 100) zip (100 to 1))
        .partitionBy(new HashPartitioner(reducerNum))
    val dep = new ShuffleDependency[Int, Int, Int](rdd, rdd.partitioner.get)
    val handle = dep.shuffleHandle.asInstanceOf[SplashBypassMergeSortShuffleHandle[Int, Int]]
    taskContext = TestUtil.newTaskContext(sc.conf)
    shuffleId = handle.shuffleId
    writer = new SplashBypassMergeSortShuffleWriter[Int, Int](
      resolver, handle, mapId, taskContext)
  }

  private def verifyMapStatus(mapStatus: Option[MapStatus]): Unit = {
    assertThat(mapStatus.isDefined) isTrue()
    mapStatus.map { s =>
      assertThat(s.location) isEqualTo resolver.blockManagerId
    }
  }

  def testWriteEmptyIterator(): Unit = {
    writer.write(Iterator.empty)
    val status = writer.stop(true)
    val lengths = writer.getPartitionLengths

    verifyMapStatus(status)
    assertThat(lengths.length) isEqualTo reducerNum
    assertThat(lengths.sum) isEqualTo 0

    assertThat(dataFile.exists()) isTrue()
    assertThat(dataFile.getSize) isEqualTo 0L

    val taskMetrics = taskContext.taskMetrics()
    val shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics
    assertThat(shuffleWriteMetrics.bytesWritten) isEqualTo 0
    assertThat(shuffleWriteMetrics.recordsWritten) isEqualTo 0
    assertThat(taskMetrics.diskBytesSpilled) isEqualTo 0
    assertThat(taskMetrics.memoryBytesSpilled) isEqualTo 0
  }

  def testWriteWithSomeEmptyPartitions(): Unit = {
    def records =
      Iterator((1, 1), (5, 5)) ++ (0 until 1000).iterator.map(_ => (2, 2))

    writer.write(records)
    val status = writer.stop(true)
    val lengths = writer.getPartitionLengths
    verifyMapStatus(status)
    assertThat(lengths.sum) isEqualTo dataFile.getSize
    assertThat(lengths.count(_ == 0L)) isEqualTo 4
    assertThat(storageFactory.getTmpFileCount) isEqualTo 0

    val taskMetrics = taskContext.taskMetrics()
    val shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics
    assertThat(shuffleWriteMetrics.bytesWritten) isEqualTo dataFile.getSize
    assertThat(shuffleWriteMetrics.recordsWritten) isEqualTo records.length
    assertThat(taskMetrics.diskBytesSpilled) isEqualTo 0
    assertThat(taskMetrics.memoryBytesSpilled) isEqualTo 0
  }

  def testCleanupTmpFilesAfterErrors(): Unit = {
    try {
      writer.write((0 until 1000).iterator.map(i => {
        if (i == 990) {
          throw new SparkException("Intentional failure")
        }
        (i, i)
      }))
    } catch {
      case _: SparkException => // expected exception, do nothing
    }
    assertThat(storageFactory.getTmpFileCount) isEqualTo 0

    writer.stop(false)
    assertThat(storageFactory.getTmpFileCount) isEqualTo 0
  }
}

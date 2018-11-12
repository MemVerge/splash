/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle

import com.memverge.splash.StorageFactoryHolder
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BlockId, ShuffleBlockId}
import org.apache.spark.{ShuffleDependency, SparkContext, TaskContext}
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations._

@Test(groups = Array("UnitTest", "IntegrationTest"))
class SplashShuffleReaderTest {
  private val appId = "test-dmo-reader-app"
  private var sc: SparkContext = _
  private val resolver = new SplashShuffleBlockResolver(appId)
  private val size = 500

  private def getData(from: Int) =
    (from until from + size).reverse.map(i => (i, i * 2)).toList

  private var rdd: RDD[(Int, Int)] = _
  private var dep: ShuffleDependency[Int, Int, Int] = _
  private var handle: BaseShuffleHandle[Int, Int, Int] = _

  private var reader: SplashShuffleReader[Int, Int] = _
  private var taskContext: TaskContext = _

  @BeforeClass
  def beforeClass(): Unit = {
    val conf = TestUtil.newBaseShuffleConf
        .set(SplashOpts.forceSpillElements, 100)
    sc = TestUtil.newSparkContext(conf)
  }

  @AfterClass
  def afterClass(): Unit = {
    sc.stop()
    StorageFactoryHolder.getFactory.reset()
  }

  @BeforeMethod
  def beforeMethod(): Unit = {
    rdd = sc.parallelize(getData(0))
        .sortByKey(ascending = false, numPartitions = 3)
    dep = new ShuffleDependency[Int, Int, Int](
      rdd,
      rdd.partitioner.get,
      keyOrdering = Some(implicitly[Ordering[Int]]))
    handle = dep.shuffleHandle.asInstanceOf[BaseShuffleHandle[Int, Int, Int]]
    taskContext = TestUtil.newTaskContext(sc.conf)
    writeData(0, getData(0))
    writeData(1, getData(3000))
    writeData(2, getData(9000))
  }

  private def writeData(mapId: Int, data: List[(Int, Int)]) = {
    val writer = new SplashShuffleWriter[Int, Int, Int](
      resolver, handle, mapId, taskContext)
    writer.write(data.iterator)
    writer.stop(true)
  }

  def testReadMetrics(): Unit = {
    reader = new SplashShuffleReader[Int, Int](
      resolver, handle, 0, 7, taskContext)
    val shuffleId = handle.shuffleId
    val reducerId = 0

    val blocks: List[(BlockId, Long)] = List[(BlockId, Long)](
      (ShuffleBlockId(shuffleId, 0, reducerId), 1),
      (ShuffleBlockId(shuffleId, 1, reducerId), 1),
      (ShuffleBlockId(shuffleId, 2, reducerId), 1))
    val actual = reader.readShuffleBlocks(blocks).toList
    var minKey = Int.MinValue
    for ((k: Int, _) <- actual) {
      assertThat(k) isGreaterThanOrEqualTo minKey
      minKey = k
    }

    val metrics = taskContext.taskMetrics()
    assertThat(metrics.memoryBytesSpilled) isEqualTo 159560L
    assertThat(metrics.diskBytesSpilled) isEqualTo 54327L
    val shuffleReadMetrics = metrics.shuffleReadMetrics
    assertThat(shuffleReadMetrics.localBlocksFetched) isEqualTo 3
    assertThat(shuffleReadMetrics.localBytesRead) isEqualTo 10189
    assertThat(shuffleReadMetrics.recordsRead) isEqualTo 1161
    assertThat(actual.length) isEqualTo 1161
  }

  def testReducer1ReadWithEmptyPartition(): Unit = {
    reader = new SplashShuffleReader[Int, Int](
      resolver, handle, 0, 7, taskContext)
    val shuffleId = handle.shuffleId
    val reducerId = 1

    val blocks: List[(BlockId, Long)] = List[(BlockId, Long)](
      (ShuffleBlockId(shuffleId, 0, reducerId), 1),
      (ShuffleBlockId(shuffleId, 1, reducerId), 0),
      (ShuffleBlockId(shuffleId, 2, reducerId), 0))
    val actual = reader.readShuffleBlocks(blocks).toList

    var minKey = Int.MinValue
    for ((k: Int, _) <- actual) {
      assertThat(k) isGreaterThanOrEqualTo minKey
      minKey = k
    }
    assertThat(actual.length) isEqualTo 173

    val shuffleReadMetrics = taskContext.taskMetrics().shuffleReadMetrics
    assertThat(shuffleReadMetrics.localBlocksFetched) isEqualTo 1
  }
}

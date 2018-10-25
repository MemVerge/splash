/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle.memverge

import com.memverge.mvfs.dmo.{DMOFile, DMOTmpFile}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark._
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations._

@Test(groups = Array("UnitTest", "IntegrationTest"))
class DMOShuffleWriterTest {
  private val appId = "test-dmo-writer-app"
  private var sc: SparkContext = _
  private val resolver = new DMOShuffleBlockResolver(appId)
  private val reducerNum = 7
  private val mapId = 1
  private var shuffleId = 0

  private var writer: DMOShuffleWriter[Int, Int, Int] = _
  private var taskContext: TaskContext = _

  @BeforeClass
  def beforeClass(): Unit = {
    sc = TestUtil.newSparkContext(TestUtil.newBaseShuffleConf)
  }

  @AfterClass
  def afterClass(): Unit = {
    sc.stop()
    DMOFile.safeReset()
  }

  @BeforeMethod
  def beforeMethod(): Unit = {
    val rdd = sc.parallelize((1 to 100) zip (100 to 1))
        .partitionBy(new HashPartitioner(reducerNum))
    val dep = new ShuffleDependency[Int, Int, Int](rdd, rdd.partitioner.get)
    val handle = dep.shuffleHandle.asInstanceOf[BaseShuffleHandle[Int, Int, Int]]
    taskContext = TestUtil.newTaskContext(sc.conf)
    shuffleId = handle.shuffleId
    writer = new DMOShuffleWriter[Int, Int, Int](
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

    val taskMetrics = taskContext.taskMetrics()
    val shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics
    assertThat(shuffleWriteMetrics.bytesWritten) isEqualTo 0
    assertThat(shuffleWriteMetrics.recordsWritten) isEqualTo 0
    assertThat(taskMetrics.diskBytesSpilled) isEqualTo 0
    assertThat(taskMetrics.memoryBytesSpilled) isEqualTo 0
  }

  def testWriteWithSomeEmptyPartitions(): Unit = {
    def records: Iterator[(Int, Int)] =
      Iterator((1, 1), (5, 5)) ++ (0 until 1000).iterator.map(_ => (2, 2))

    writer.write(records)
    val dataFile = resolver.getDataFile(shuffleId, mapId)
    val status = writer.stop(true)
    val lengths = writer.getPartitionLengths

    verifyMapStatus(status)
    assertThat(lengths.sum) isEqualTo dataFile.getSize
    assertThat(lengths.count(_ == 0L)) isEqualTo 4
    assertThat(DMOTmpFile.listAll().length) isEqualTo 0

    val taskMetrics = taskContext.taskMetrics()
    val shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics
    assertThat(shuffleWriteMetrics.bytesWritten) isEqualTo dataFile.getSize
    assertThat(shuffleWriteMetrics.recordsWritten) isEqualTo records.length
    assertThat(taskMetrics.diskBytesSpilled) isEqualTo 0
    assertThat(taskMetrics.memoryBytesSpilled) isEqualTo 0
  }

  def testNoTmpFileIfNotSpill(): Unit = {
    def records: Iterator[(Int, Int)] =
      Iterator((1, 1), (5, 5)) ++
          (0 until 1000).iterator.map { i =>
            if (i == 999) throw TestUtil.IntentionalFailure() else (2, 2)
          }

    try {
      writer.write(records)
    } catch {
      case _: SparkException => None
    }
    assertThat(DMOTmpFile.listAll().length) isEqualTo 0

    writer.stop(false)
    assertThat(DMOTmpFile.listAll().length) isEqualTo 0
  }
}

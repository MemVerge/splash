/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package org.apache.spark.shuffle.sort

import com.memverge.mvfs.dmo.{DMOFile, DMOInputStream, DMOTmpFile}
import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.io.{LZ4CompressionCodec, LZFCompressionCodec, SnappyCompressionCodec}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.serializer.{KryoSerializer, Serializer, SerializerManager}
import org.apache.spark.shuffle.memverge._
import org.assertj.core.api.Assertions.{assertThat, assertThatExceptionOfType}
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.testng.annotations._

import scala.collection.AbstractIterator
import scala.collection.mutable.ArrayBuffer

@Test(groups = Array("UnitTest", "IntegrationTest"))
class DMOUnsafeShuffleWriterTest {
  private val appId = "test-dmo-unsafe-writer-app"
  private var sc: SparkContext = _
  private val resolver = new DMOShuffleBlockResolver(appId)
  private val reducerNum = 4
  private val hashPartitioner = new HashPartitioner(reducerNum)
  private val mapId = 1
  private var shuffleId = 0

  private var taskContext: TaskContext = _
  private var serializer: Serializer = _

  @BeforeClass
  def beforeClass(): Unit = {
    sc = TestUtil.newSparkContext(TestUtil.newSparkConf()
        .set("spark.serializer", classOf[KryoSerializer].getName))
  }

  @AfterClass
  def afterClass(): Unit = {
    sc.stop()
    DMOFile.safeReset()
  }

  private def createWriter(
      serializerManager: SerializerManager = SparkEnv.get.serializerManager) = {
    val rdd = sc.parallelize(0 until 1000).map { i => (i / 2, i) }
        .partitionBy(new HashPartitioner(reducerNum))
    serializer = new KryoSerializer(sc.conf)
    val dep = new ShuffleDependency[Int, Int, Int](rdd, rdd.partitioner.get)
    val handle = dep.shuffleHandle.asInstanceOf[DMOSerializedShuffleHandle[Int, Int]]
    taskContext = TestUtil.newTaskContext(sc.conf)
    shuffleId = handle.shuffleId
    new DMOUnsafeShuffleWriter[Int, Int](
      resolver, handle, mapId, taskContext,
      DMOSerializer(serializerManager, handle.dependency))
  }

  @BeforeMethod
  def beforeMethod(): Unit = {
    sc.conf
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set(MVFSOpts.shuffleCompress, true)
        .set("spark.io.compression.codec", "lz4")
        .set(config.IO_ENCRYPTION_ENABLED, false)
        .set(MVFSOpts.forceSpillElements, Int.MaxValue)
        .set(MVFSOpts.mvfsShuffleFileBufferKB, 4096L)
  }

  @AfterMethod
  def afterMethod(): Unit = {
    DMOFile.safeReset()
  }

  private def verifyMapStatus(mapStatus: Option[MapStatus]): Unit = {
    assertThat(mapStatus.isDefined) isTrue()
    mapStatus.map { s =>
      assertThat(s.location) isEqualTo resolver.blockManagerId
    }
  }

  private def readRecordsFromFile(
      writer: DMOUnsafeShuffleWriter[Int, Int]): List[(Int, Int)] = {
    val recordsList = new ArrayBuffer[(Int, Int)]()
    var startOffset = 0L
    (0 until reducerNum).foreach { i =>
      val partitionSize = writer.getPartitionLengths(i)
      if (partitionSize > 0) {
        val dmoFile = resolver.getDataFile(shuffleId, mapId)
        val endOffset = startOffset + partitionSize
        DMOUtils.withResources(new DMOInputStream(dmoFile, endOffset)) { is =>
          is.seek(startOffset)
          val in = writer.wrap(is)
          val recordsIs = serializer.newInstance().deserializeStream(in)
          val recordsIterator = recordsIs.asKeyValueIterator
          while (recordsIterator.hasNext) {
            val record = recordsIterator.next().asInstanceOf[(Int, Int)]
            assertThat(hashPartitioner.getPartition(record._1)) isEqualTo i
            recordsList.append(record)
          }
          recordsIs.close()
          startOffset += partitionSize
        }
      }
    }
    recordsList.toList
  }

  def testMustCallWriteBeforeSuccessfulStop(): Unit = {
    assertThatExceptionOfType(classOf[IllegalStateException])
        .isThrownBy(new ThrowingCallable {
          override def call(): Unit = createWriter().stop(true)
        })
  }

  def testDoNotNeedToCallWriteBeforeUnsuccessfulStop(): Unit = {
    assertThat(createWriter().stop(false)) isEqualTo None
  }

  case class IntentionalException() extends RuntimeException

  def testWriteFailurePropagates(): Unit = {
    class BadRecords extends AbstractIterator[Product2[Int, Int]] {
      override def hasNext(): Boolean = throw new IntentionalException

      override def next(): Product2[Int, Int] = null
    }

    assertThatExceptionOfType(classOf[IntentionalException])
        .isThrownBy(new ThrowingCallable {
          override def call(): Unit = createWriter().write(new BadRecords())
        })
  }

  def testWriteEmptyIterator(): Unit = {
    val writer = createWriter()
    writer.write(List[Product2[Int, Int]]().iterator)
    val status = writer.stop(true)
    verifyMapStatus(status)
    assertThat(writer.getPartitionLengths) isEqualTo new Array[Long](reducerNum)

    val taskMetrics = taskContext.taskMetrics()
    assertThat(taskMetrics.shuffleWriteMetrics.recordsWritten) isEqualTo 0
    assertThat(taskMetrics.shuffleWriteMetrics.bytesWritten) isEqualTo 0
    assertThat(taskMetrics.diskBytesSpilled) isEqualTo 0
    assertThat(taskMetrics.memoryBytesSpilled) isEqualTo 0
    assertThat(DMOTmpFile.listAll()) hasSize 0
  }

  def testWriteWithOutSpilling(): Unit = {
    val writer = createWriter()
    val dataToWrite = new ArrayBuffer[Product2[Int, Int]]()
    (0 until reducerNum) foreach { i =>
      dataToWrite append ((i, i))
    }
    writer.write(dataToWrite.iterator)
    val status = writer.stop(true)
    verifyMapStatus(status)

    val sumOfPartitionSizes = writer.getPartitionLengths.sum
    val dataFileSize = resolver.getDataFile(shuffleId, mapId).getSize
    assertThat(sumOfPartitionSizes) isEqualTo dataFileSize
    assertThat(DMOTmpFile.listAll()) hasSize 0
    assertThat(dataToWrite) isEqualTo readRecordsFromFile(writer)

    val taskMetrics = taskContext.taskMetrics()
    val shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics
    assertThat(taskMetrics.diskBytesSpilled) isEqualTo 0
    assertThat(taskMetrics.memoryBytesSpilled) isEqualTo 0
    assertThat(shuffleWriteMetrics.bytesWritten) isEqualTo dataFileSize
  }

  private def testMergingSpills(
      compressionCodecName: String, encrypt: Boolean): Unit = {
    val conf = sc.conf
    if (compressionCodecName != null) {
      conf.set(MVFSOpts.shuffleCompress, true)
      conf.set("spark.io.compression.codec", compressionCodecName)
    } else {
      conf.set(MVFSOpts.shuffleCompress, false)
    }
    conf.set(config.IO_ENCRYPTION_ENABLED, encrypt)

    val serializerManager = if (encrypt) {
      new SerializerManager(serializer, conf,
        Option.apply(CryptoStreamUtils.createKey(conf)))
    } else {
      new SerializerManager(serializer, conf)
    }

    val writer = createWriter(serializerManager)
    testMergingSpills(writer, encrypt)
  }

  private def testMergingSpills(
      writer: DMOUnsafeShuffleWriter[Int, Int], encrypt: Boolean): Unit = {
    val dataToWrite = List(1, 2, 3, 4, 4, 2).map(i => (i, i))
    (0 to 3).foreach(i => writer.insertRecordIntoSorter(dataToWrite(i)))
    writer.forceSorterToSpill()
    assertThat(DMOTmpFile.listAll()) hasSize 1
    (4 to 5).foreach(i => writer.insertRecordIntoSorter(dataToWrite(i)))
    writer.closeAndWriteOutput()
    val mapStatus = writer.stop(true)
    verifyMapStatus(mapStatus)
    assertThat(DMOTmpFile.listAll()) hasSize 0

    val sumOfPartitionSizes = writer.getPartitionLengths.sum
    val dataFileSize = resolver.getDataFile(shuffleId, mapId).getSize
    assertThat(sumOfPartitionSizes) isEqualTo dataFileSize
    assertThat(dataToWrite.toSet) isEqualTo readRecordsFromFile(writer).toSet

    val taskMetrics = taskContext.taskMetrics()
    val shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics
    assertThat(taskMetrics.diskBytesSpilled) isGreaterThan 0
    assertThat(taskMetrics.diskBytesSpilled) isLessThan dataFileSize
    assertThat(taskMetrics.memoryBytesSpilled) isGreaterThan 0
    assertThat(shuffleWriteMetrics.bytesWritten) isEqualTo dataFileSize
  }

  def testMergeSpillsWithLZF(): Unit =
    testMergingSpills(classOf[LZFCompressionCodec].getName, encrypt = false)

  def testMergeSpillsWithLZ4(): Unit =
    testMergingSpills(classOf[LZ4CompressionCodec].getName, encrypt = false)

  def testMergeSpillsWithSnappy(): Unit =
    testMergingSpills(classOf[SnappyCompressionCodec].getName, encrypt = false)

  def testMergeSpillsWithoutCompression(): Unit =
    testMergingSpills(null: String, encrypt = false)

  def testMergeSpillsWithLZ4AndEncryption(): Unit =
    testMergingSpills(classOf[LZ4CompressionCodec].getName, encrypt = true)

  def testMergeSpillsWithEncryptionAndNoCompression(): Unit =
    testMergingSpills(null: String, encrypt = true)

  def testWriteEnoughDataToTriggerSpill(): Unit = {
    val size = 100
    sc.conf.set(MVFSOpts.forceSpillElements, size / 2)
    val writer = createWriter()
    val dataToWrite = (1 to size).map(i => (i, i * 2))
    writer.write(dataToWrite.iterator)
    assertThat(writer.getSpilled) isEqualTo 2
    writer.stop(true)
    assertThat(dataToWrite.toSet) isEqualTo readRecordsFromFile(writer).toSet

    val dataFileSize = resolver.getDataFile(shuffleId, mapId).getSize
    val taskMetrics = taskContext.taskMetrics()
    val shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics
    assertThat(dataToWrite.size) isEqualTo shuffleWriteMetrics.recordsWritten
    assertThat(taskMetrics.diskBytesSpilled) isGreaterThan 0L
    assertThat(taskMetrics.diskBytesSpilled) isLessThan dataFileSize
    assertThat(taskMetrics.memoryBytesSpilled) isGreaterThan 0L
    assertThat(dataFileSize) isEqualTo shuffleWriteMetrics.bytesWritten
  }

  def testWriteRecordsThatAreBiggerThanDiskWriteBufferSize(): Unit = {
    sc.conf.set(MVFSOpts.mvfsShuffleFileBufferKB, 512L)
    val size = 2048
    val writer = createWriter()
    val dataToWrite = (1 to size).map(i => (i, i * 2))
    writer.write(dataToWrite.iterator)
    writer.stop(true)
    assertThat(dataToWrite.toSet) isEqualTo readRecordsFromFile(writer).toSet
  }

  def testSpillFilesAreDeletedWhenStoppingAfterError(): Unit = {
    val writer = createWriter()
    writer.insertRecordIntoSorter((1, 2))
    writer.insertRecordIntoSorter((3, 4))
    writer.forceSorterToSpill()
    writer.insertRecordIntoSorter((3, 2))
    writer.stop(false)
    assertThat(DMOTmpFile.listAll()) hasSize 0
  }
}

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

import com.memverge.splash.StorageFactoryHolder
import org.assertj.core.api.Assertions.{assertThat, assertThatExceptionOfType}
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.testng.annotations.{AfterMethod, BeforeMethod, Test}

import scala.util.Random

@Test(groups = Array("UnitTest", "IntegrationTest"))
class SpilledFileTest {
  private val factory = StorageFactoryHolder.getFactory
  private val originalBatchSize = SpilledFile.serializeBatchSize
  private val serializer: SplashSerializer = TestUtil.kryoSerializer

  @BeforeMethod
  private def beforeMethod(): Unit = {
    SpilledFile.serializeBatchSize = originalBatchSize
  }

  @AfterMethod
  private def afterMethod(): Unit = {
    factory.reset()
    assertThat(factory.getTmpFileCount).isEqualTo(0)
  }

  def testFailedSpilledFile(): Unit = {
    val tmpFile = factory.makeSpillFile()
    tmpFile.recall()
    val spilledFile = SpilledFile(tmpFile)
    assertThat(spilledFile.bytesSpilled) isEqualTo 0
  }

  private def partitionedArray = Array[((Int, Int), String)](
    ((0, 4), "04"),
    ((2, 2), "22"),
    ((2, 3), "23"),
    ((3, 6), "36"))

  private def partitionedData = {
    WritablePartitionedIterator(partitionedArray.iterator, 4)
  }

  def testBatchSizes(): Unit = {
    val file = SpilledFile(factory.makeSpillFile(),
      Array(0, 12, 42, 57, 89))
    assertThat(file.bytesSpilled).isEqualTo(89)
    assertThat(file.batchSizes).isEqualTo(Array[Long](12, 30, 15, 32))
  }

  def testPartitionedSpillIterator(): Unit = {
    SpilledFile.serializeBatchSize = 2
    val spill = SpilledFile.spill(partitionedData, serializer)
    val spillReader = new PartitionedSpillReader(spill, serializer)
    assertThat(spillReader.toArray).isEqualTo(
      Array((4, "04"), (2, "22"), (3, "23"), (6, "36")))
  }

  def testPartitionedSpill(): Unit = {
    val spill = SpilledFile.spill(partitionedData, serializer)
    assertThat(spill.batchOffsets).isEqualTo(Array[Long](0, 66))
    assertThat(spill.elementsPerPartition).isEqualTo(Array[Long](1, 0, 2, 1))
    val spillReader = new PartitionedSpillReader(spill, serializer)
    assertThat(spillReader.iterator().toArray).isEqualTo(partitionedArray)
  }

  def testPartitionedSpillWithMultipleBatches(): Unit = {
    SpilledFile.serializeBatchSize = 2
    val spill = SpilledFile.spill(partitionedData, serializer)
    assertThat(spill.batchOffsets).isEqualTo(Array[Long](0, 54, 108))
    assertThat(spill.bytesSpilled).isEqualTo(108)
    assertThat(spill.elementsPerPartition).isEqualTo(Array[Long](1, 0, 2, 1))
  }

  def testReadPartitionedSpillWithMultipleBatches(): Unit = {
    SpilledFile.serializeBatchSize = 2
    val spill = SpilledFile.spill(partitionedData, serializer)
    val spillReader = new PartitionedSpillReader(spill, serializer)
    assertThat(spillReader.iterator().toArray).isEqualTo(partitionedArray)
  }

  def testReadPartitionedSpillTwice(): Unit = {
    val spill = SpilledFile.spill(partitionedData, serializer)

    val spillReader0 = new PartitionedSpillReader(spill, serializer)
    assertThat(spillReader0.iterator().toArray).isEqualTo(partitionedArray)

    val spillReader1 = new PartitionedSpillReader(spill, serializer)
    assertThat(spillReader1.iterator().toArray).isEqualTo(partitionedArray)
  }

  def testReadNextPartition(): Unit = {
    SpilledFile.serializeBatchSize = 2
    val spill = SpilledFile.spill(partitionedData, serializer)
    val spillReader = new PartitionedSpillReader(spill, serializer)
    val p0 = spillReader.readNextPartition()
    assertThat(p0.toArray).isEqualTo(Array((4, "04")))

    val p1 = spillReader.readNextPartition()
    assertThat(p1.isEmpty).isTrue

    val p2 = spillReader.readNextPartition()
    assertThat(p2.toArray).isEqualTo(Array((2, "22"), (3, "23")))

    val p3 = spillReader.readNextPartition()
    assertThat(p3.toArray).isEqualTo(Array((6, "36")))

    val p4 = spillReader.readNextPartition()
    assertThat(p4.isEmpty).isTrue
  }

  def testReadNextPartitionInNormalSequence(): Unit = {
    SpilledFile.serializeBatchSize = 2
    val spill = SpilledFile.spill(partitionedData, serializer)
    val spillReader = new PartitionedSpillReader(spill, serializer)
    val p0 = spillReader.readNextPartition()
    val p1 = spillReader.readNextPartition()
    val p2 = spillReader.readNextPartition()
    val p3 = spillReader.readNextPartition()
    val p4 = spillReader.readNextPartition()

    assertThat(p0.toArray).isEqualTo(Array((4, "04")))
    assertThat(p1.isEmpty).isTrue
    assertThat(p2.toArray).isEqualTo(Array((2, "22"), (3, "23")))
    assertThat(p3.toArray).isEqualTo(Array((6, "36")))
    assertThat(p4.isEmpty).isTrue
  }

  def testSpillSizeWithoutBatchSize(): Unit = {
    val spill = factory.makeSpillFile()
    val text = "hello"
    SplashUtils.withResources(spill.makeOutputStream()) { os =>
      os.write(text.getBytes())
    }
    val spilledFile = SpilledFile(spill)
    assertThat(spilledFile.bytesSpilled).isEqualTo(text.length)
  }

  private def pairData: Iterator[(Int, String)] = {
    Array((4, "s4"),
      (2, "s2"),
      (3, "s3"),
      (6, "s6"),
      (7, "s7")).iterator
  }

  def testPairSpill(): Unit = {
    val spill = SpilledFile.spill(pairData, serializer)
    assertThat(spill.bytesSpilled).isEqualTo(72)
    assertThat(spill.batchOffsets).isEqualTo(Array[Long](0, 72))
    assertThat(spill.elementsPerPartition).isEqualTo(Array.empty[Long])
  }

  def testReadSpilledPair(): Unit = {
    val spill = SpilledFile.spill(pairData, serializer)
    val iterator = new PairSpillReader[Int, String](spill, serializer)
    assertThat(iterator.toArray).isEqualTo(pairData.toArray)
  }

  def testReadSpilledPairTwice(): Unit = {
    val spill = SpilledFile.spill(pairData, serializer)
    val iterator0 = new PairSpillReader[Int, String](spill, serializer)
    assertThat(iterator0.toArray).isEqualTo(pairData.toArray)

    val iterator1 = new PairSpillReader[Int, String](spill, serializer)
    assertThat(iterator1.toArray).isEqualTo(pairData.toArray)
  }

  def testPairSpillWithMultipleBatches(): Unit = {
    SpilledFile.serializeBatchSize = 2
    val spill = SpilledFile.spill(pairData, serializer)
    assertThat(spill.bytesSpilled).isEqualTo(156)
    assertThat(spill.batchOffsets).isEqualTo(Array[Long](0, 54, 108, 156))
    val iterator = new PairSpillReader[Int, String](spill, serializer)
    assertThat(iterator.toArray).isEqualTo(pairData.toArray)
  }

  def testReadLimitedPairSpill(): Unit = {
    SpilledFile.serializeBatchSize = 1
    val spill = SpilledFile.spill(pairData, serializer)
    val iterator = new PairSpillReader[Int, String](spill, serializer).limit(1, 3)
    assertThat(iterator.toArray).isEqualTo(Array((2, "s2"), (3, "s3")))
  }

  def testLimitNoDataAvailable(): Unit = {
    val spill = SpilledFile.spill(pairData, serializer)
    val iterator = new PairSpillReader[Int, String](spill, serializer).limit(1, 2)
    assertThat(iterator.isEmpty).isTrue
    assertThatExceptionOfType(classOf[NoSuchElementException])
        .isThrownBy(new ThrowingCallable {
          override def call(): Unit = iterator.next()
        })
  }

  def testRecallSpillFileIfNoSpill(): Unit = {
    val spill = SpilledFile.spill(Iterator.empty, serializer)
    assertThat(spill.file.exists()).isFalse
  }

  @Test(enabled = false)
  def testSpillReaderPerformance(): Unit = {
    val size = 20000000L
    val rand = new Random(1)
    val data = (1L to size).map(i => (i, rand.nextLong()))
    val spill = SpilledFile.spill(data.iterator, serializer)
    var theSum = 0L
    TestUtil.time {
      val iterator = new PairSpillReader[Long, Long](spill, serializer)
      theSum = iterator.map(x => x._1).sum
      assertThat(theSum).isEqualTo(200000010000000L)
    }(50)
  }
}

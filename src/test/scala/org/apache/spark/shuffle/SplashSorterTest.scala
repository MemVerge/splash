/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle

import com.memverge.splash.StorageFactoryHolder
import org.apache.spark._
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.{AfterClass, AfterMethod, DataProvider, Test}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


@Test(groups = Array("UnitTest", "IntegrationTest"))
class SplashSorterTest {
  private var sc: SparkContext = _
  private var sorter: SplashSorter[Int, Int, Int] = _
  private var strSorter: SplashSorter[String, String, String] = _
  private val storageFactory = StorageFactoryHolder.getFactory

  @AfterMethod
  def afterMethod(): Unit = {
    if (sorter != null) {
      sorter.stop()
      sorter = null
    }
    if (strSorter != null) {
      strSorter.stop()
      strSorter = null
    }
    if (sc != null) {
      sc.stop()
      sc = null
    }
  }

  @AfterClass
  def afterClass(): Unit = {
    storageFactory.reset()
  }

  private val confWithKryo = TestUtil.newBaseShuffleConf
      .set("spark.serializer", classOf[KryoSerializer].getName)
  private val confWithoutKryo = TestUtil.newBaseShuffleConf
      .set("spark.serializer.objectStreamReset", "1")
      .set("spark.serializer", classOf[JavaSerializer].getName)

  @DataProvider(name = "sparkConfSerializer")
  def sparkConfWithDifferentSer: Array[SparkConf] = {
    Array(confWithKryo, confWithoutKryo)
  }

  @Test(dataProvider = "sparkConfSerializer")
  def testEmptyDataStreamBothAggAndOrd(conf: SparkConf): Unit = {
    sc = TestUtil.newSparkContext(conf)

    sorter = TestUtil.newDMOSpillSorter[Int, Int, Int](
      Some(TestUtil.sumAgg),
      Some(new HashPartitioner(3)),
      Some(implicitly[Ordering[Int]]),
      sc.conf)

    assertThat(sorter.toSeq) isEqualTo Seq()
  }

  @Test(dataProvider = "sparkConfSerializer")
  def testEmptyDataStreamOnlyAgg(conf: SparkConf): Unit = {
    sc = TestUtil.newSparkContext(conf)

    sorter = TestUtil.newDMOSpillSorter[Int, Int, Int](
      Some(TestUtil.sumAgg),
      Some(new HashPartitioner(3)),
      None,
      sc.conf)

    assertThat(sorter.toSeq) isEqualTo Seq()
  }

  @Test(dataProvider = "sparkConfSerializer")
  def testEmptyDataStreamOnlyOrd(conf: SparkConf): Unit = {
    sc = TestUtil.newSparkContext(conf)

    sorter = TestUtil.newDMOSpillSorter[Int, Int, Int](
      None,
      Some(new HashPartitioner(3)),
      Some(implicitly[Ordering[Int]]),
      sc.conf)

    assertThat(sorter.toSeq) isEqualTo Seq()
  }

  @Test(dataProvider = "sparkConfSerializer")
  def testEmptyDataStreamNoAggOrOrd(conf: SparkConf): Unit = {
    sc = TestUtil.newSparkContext(conf)

    sorter = TestUtil.newDMOSpillSorter[Int, Int, Int](
      None,
      Some(new HashPartitioner(3)),
      None,
      sc.conf)

    assertThat(sorter.toSeq) isEqualTo Seq()
  }

  @Test(dataProvider = "sparkConfSerializer")
  def testFewElementsPerPartitionBothAggAndOrd(conf: SparkConf): Unit = {
    sc = TestUtil.newSparkContext(conf)

    sorter = TestUtil.newDMOSpillSorter[Int, Int, Int](
      Some(TestUtil.sumAgg),
      Some(new HashPartitioner(7)),
      Some(implicitly[Ordering[Int]]),
      sc.conf)

    val elements = Set((1, 1), (2, 2), (5, 5))
    val expected = Set(
      (0, Set()),
      (1, Set((1, 1))),
      (2, Set((2, 2))),
      (3, Set()),
      (4, Set()),
      (5, Set((5, 5))),
      (6, Set()))

    sorter.insertAll(elements.iterator)
    assertThat(sorter.toSet) isEqualTo expected
  }

  @Test(dataProvider = "sparkConfSerializer")
  def testFewElementsPerPartitionOnlyAgg(conf: SparkConf): Unit = {
    sc = TestUtil.newSparkContext(conf)

    sorter = TestUtil.newDMOSpillSorter[Int, Int, Int](
      Some(TestUtil.sumAgg),
      Some(new HashPartitioner(7)),
      None,
      sc.conf)

    val elements = Set((1, 1), (2, 2), (5, 5))
    val expected = Set(
      (0, Set()),
      (1, Set((1, 1))),
      (2, Set((2, 2))),
      (3, Set()),
      (4, Set()),
      (5, Set((5, 5))),
      (6, Set()))

    sorter.insertAll(elements.iterator)
    assertThat(sorter.toSet) isEqualTo expected
  }

  @Test(dataProvider = "sparkConfSerializer")
  def testFewElementsPerPartitionOnlyOrd(conf: SparkConf): Unit = {
    sc = TestUtil.newSparkContext(conf)

    sorter = TestUtil.newDMOSpillSorter[Int, Int, Int](
      None,
      Some(new HashPartitioner(7)),
      Some(implicitly[Ordering[Int]]),
      sc.conf)

    val elements = Set((1, 1), (2, 2), (5, 5))
    val expected = Set(
      (0, Set()),
      (1, Set((1, 1))),
      (2, Set((2, 2))),
      (3, Set()),
      (4, Set()),
      (5, Set((5, 5))),
      (6, Set()))

    sorter.insertAll(elements.iterator)
    assertThat(sorter.toSet) isEqualTo expected
  }

  @Test(dataProvider = "sparkConfSerializer")
  def testFewElementsPerPartitionNoAggOrOrd(conf: SparkConf): Unit = {
    sc = TestUtil.newSparkContext(conf)

    sorter = TestUtil.newDMOSpillSorter[Int, Int, Int](
      None,
      Some(new HashPartitioner(7)),
      None,
      sc.conf)

    val elements = Set((1, 1), (2, 2), (5, 5))
    val expected = Set(
      (0, Set()),
      (1, Set((1, 1))),
      (2, Set((2, 2))),
      (3, Set()),
      (4, Set()),
      (5, Set((5, 5))),
      (6, Set()))

    sorter.insertAll(elements.iterator)
    assertThat(sorter.toSet) isEqualTo expected
  }

  private def newSparkContextWithForceSpillSize(
      conf: SparkConf, size: Int): SparkContext = {
    conf.set(SplashOpts.forceSpillElements, size)
    TestUtil.newSparkContext(conf)
  }

  @Test(dataProvider = "sparkConfSerializer")
  def testEmptyPartitionsWithSpilling(conf: SparkConf): Unit = {
    val size = 1000
    sc = newSparkContextWithForceSpillSize(conf, size / 2)
    val elements = Iterator((1, 1), (5, 5)) ++
        (0 until size).iterator.map(_ => (2, 2))

    sorter = TestUtil.newDMOSpillSorter[Int, Int, Int](
      None,
      Some(new HashPartitioner(7)),
      Some(implicitly[Ordering[Int]]),
      sc.conf)
    sorter.insertAll(elements)

    assertThat(sorter.numSpills) isGreaterThan 0
    val it = sorter.partitionedIterator.map(p => (p._1, p._2.toList))
    assertThat(it.next()) isEqualTo ((0, Nil))
    assertThat(it.next()) isEqualTo ((1, List((1, 1))))
    assertThat(it.next()) isEqualTo ((2, (0 until size).map(_ => (2, 2)).toList))
    assertThat(it.next()) isEqualTo ((3, Nil))
    assertThat(it.next()) isEqualTo ((4, Nil))
    assertThat(it.next()) isEqualTo ((5, List((5, 5))))
    assertThat(it.next()) isEqualTo ((6, Nil))
  }

  @DataProvider(name = "confWithReducerNumList")
  def confWithReducerNumList: Array[ConfWithReducerN] = {
    sparkConfWithDifferentSer.flatMap { conf =>
      List(ConfWithReducerN(conf, 2),
        ConfWithReducerN(conf, 100))
    }
  }

  @Test(dataProvider = "confWithReducerNumList")
  def testSpillLocalReduceByKey(conf: ConfWithReducerN): Unit = {
    val size = 5000
    sc = newSparkContextWithForceSpillSize(conf.conf, size / 4)
    TestUtil.assertSpilled(sc) {
      val result = sc.parallelize(0 until size)
          .map { i => (i / 2, i) }
          .reduceByKey(math.max _, conf.numReducer)
          .collect()
      assertThat(result.length) isEqualTo size / 2
      result foreach { case (k, v) =>
        val expected = k * 2 + 1
        val msg = s"Value for $k was wrong: expected $expected, got $v"
        assertThat(v) isEqualTo expected describedAs msg
      }
    }
  }

  @Test(dataProvider = "confWithReducerNumList")
  def testSpillLocalGroupByKey(conf: ConfWithReducerN): Unit = {
    val size = 5000
    sc = newSparkContextWithForceSpillSize(conf.conf, size / 4)
    TestUtil.assertSpilled(sc) {
      val result = sc.parallelize(0 until size)
          .map { i => (i / 2, i) }
          .groupByKey(conf.numReducer)
          .collect()
      assertThat(result.length) isEqualTo size / 2
      result foreach { case (i, seq) =>
        val actual = seq.toSet
        val expected = Set(i * 2, i * 2 + 1)
        val msg = s"Value for $i was wrong: expected $expected, got $actual"
        assertThat(actual) isEqualTo expected describedAs msg
      }
    }
  }

  @Test(dataProvider = "confWithReducerNumList")
  def testSpillLocalCoGroup(conf: ConfWithReducerN): Unit = {
    val size = 5000
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

  @Test(dataProvider = "confWithReducerNumList")
  def testSpillLocalSortByKey(conf: ConfWithReducerN): Unit = {
    val size = 5000
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

  case class FailuresOption(injectFailure: Boolean)

  @DataProvider(name = "failuresOptions")
  def failuresOptions: Array[FailuresOption] = {
    Array(FailuresOption(true), FailuresOption(false))
  }

  @Test(dataProvider = "failuresOptions")
  def testCleanupIntermediateFilesInSorter(option: FailuresOption): Unit = {
    val size = 1200
    val withFailures = option.injectFailure
    sc = newSparkContextWithForceSpillSize(confWithoutKryo, size / 4)
    val ord = implicitly[Ordering[Int]]
    val expectedSize = if (withFailures) size - 1 else size
    val context = TestUtil.newTaskContext(confWithoutKryo)
    val sorter = new SplashSorter[Int, Int, Int](
      context, None, Some(new HashPartitioner(3)), Some(ord))
    if (withFailures) {
      try {
        sorter.insertAll((0 until size).iterator.map { i =>
          if (i == size - 1) {
            throw TestUtil.IntentionalFailure()
          }
          (i, i)
        })
      } catch {
        case _: Exception => None
      }
    } else {
      sorter.insertAll((0 until size).iterator.map(i => (i, i)))
    }

    assertThat(sorter.iterator.toSet) isEqualTo (0 until expectedSize).map(i => (i, i)).toSet
    assertThat(sorter.numSpills) isGreaterThan 0
    assertThat(storageFactory.getTmpFileCount) isGreaterThan 0
    sorter.stop()
    assertThat(storageFactory.getTmpFileCount) isEqualTo 0
  }

  @Test(dataProvider = "failuresOptions")
  def testCleanupIntermediateFilesInShuffle(option: FailuresOption): Unit = {
    val size = 1200
    val withFailures = option.injectFailure
    sc = newSparkContextWithForceSpillSize(confWithoutKryo, size / 4)
    val data = sc.parallelize(0 until size, 2).map { i =>
      if (withFailures && i == size - 1) {
        throw TestUtil.IntentionalFailure()
      }
      (i, i)
    }

    TestUtil.assertSpilled(sc) {
      if (withFailures) {
        try {
          data.reduceByKey(_ + _).count()
        } catch {
          case _: Exception => None
        }
        // contains shuffle data, index
        assertThat(storageFactory.getShuffleFileCount(sc.applicationId)) isEqualTo 2
      } else {
        assertThat(data.reduceByKey(_ + _).count()) isEqualTo size
        // contains shuffle data and index for two slices
        assertThat(storageFactory.getShuffleFileCount(sc.applicationId)) isEqualTo 4
      }
    }
  }

  case class SorterTestOption(
      conf: SparkConf,
      withPartialAgg: Boolean,
      withOrdering: Boolean,
      withSpilling: Boolean)

  @DataProvider(name = "sorterTestOptions")
  def sorterTestOptions(): Array[SorterTestOption] = {
    sparkConfWithDifferentSer.flatMap { conf =>
      (0 until 8).map { i =>
        SorterTestOption(conf,
          withPartialAgg = (i & 4) > 0,
          withOrdering = (i & 2) > 0,
          withSpilling = (i & 1) > 0)
      }
    }
  }

  @Test(dataProvider = "sorterTestOptions")
  def testSorter(option: SorterTestOption): Unit = {
    val SorterTestOption(conf, withPartialAgg, withOrdering, withSpilling) = option
    val size = 1000
    if (withSpilling) {
      conf.set(SplashOpts.forceSpillElements, size / 2)
    } else {
      conf.set(SplashOpts.forceSpillElements, Int.MaxValue)
    }
    sc = TestUtil.newSparkContext(conf)
    val agg = if (withPartialAgg) Some(TestUtil.sumAgg) else None
    val ord = if (withOrdering) Some(implicitly[Ordering[Int]]) else None
    sorter = TestUtil.newDMOSpillSorter[Int, Int, Int](
      agg,
      Some(new HashPartitioner(3)),
      ord,
      sc.conf)
    sorter.insertAll((0 until size).iterator.map { i => (i / 4, i) })

    if (withSpilling) {
      assertThat(sorter.numSpills) isGreaterThan 0
    } else {
      assertThat(sorter.numSpills) isEqualTo 0
    }

    val results = sorter.partitionedIterator.map {
      case (p, vs) => (p, vs.toSet)
    }.toSet

    val expected = (0 until 3).map { p =>
      var v = (0 until size)
          .map { i => (i / 4, i) }
          .filter { case (k, _) => k % 3 == p }
          .toSet
      if (withPartialAgg) {
        v = v.groupBy(_._1).mapValues { s => s.map(_._2).sum }.toSet
      }
      (p, v)
    }.toSet
    assertThat(results) isEqualTo expected
  }

  @Test(dataProvider = "sparkConfSerializer",
    expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testSortBreakingSortingContracts(conf: SparkConf): Unit = {
    val size = 10000
    sc = newSparkContextWithForceSpillSize(conf, size / 2)

    // Using wrongOrdering to show integer overflow introduced exception.
    val rand = new Random(100L)
    val wrongOrdering = new Ordering[String] {
      override def compare(a: String, b: String): Int = {
        val h1 = if (a == null) 0 else a.hashCode()
        val h2 = if (b == null) 0 else b.hashCode()
        h1 - h2
      }
    }

    val testData = Array.tabulate(size) { _ => rand.nextInt().toString }
    val context = TestUtil.newTaskContext(conf)
    strSorter = new SplashSorter[String, String, String](
      context, None, None, Some(wrongOrdering))

    strSorter.insertAll(testData.iterator.map(i => (i, i)))
    strSorter.iterator
  }

  @Test(dataProvider = "sparkConfSerializer")
  def testSortWithoutBreakingSortingContracts(conf: SparkConf): Unit = {
    val size = 1000
    sc = newSparkContextWithForceSpillSize(conf, size / 2)

    var value = size
    val testData = Array.tabulate(size) { _ =>
      val ret = value
      value -= 1
      ret
    }

    def createCombiner(i: Int): ArrayBuffer[Int] = ArrayBuffer(i)

    def mergeValue(c: ArrayBuffer[Int], i: Int): ArrayBuffer[Int] = c += i

    def mergeCombiners(c1: ArrayBuffer[Int], c2: ArrayBuffer[Int]): ArrayBuffer[Int] =
      c1 ++= c2

    val agg = new Aggregator[Int, Int, ArrayBuffer[Int]](
      createCombiner, mergeValue, mergeCombiners)

    val context = TestUtil.newTaskContext(sc.conf)
    val testSorter = new SplashSorter[Int, Int, ArrayBuffer[Int]](
      context, Some(agg), None, None)

    testSorter.insertAll(testData.iterator.map(i => (i, i)))
    assertThat(testSorter.numSpills) isGreaterThan 0

    var minKey = Int.MinValue
    val iterator = testSorter.iterator
    iterator.foreach { case Product2(k, _) =>
      assertThat(k) isGreaterThanOrEqualTo minKey
      minKey = k
    }
    testSorter.stop()
  }

  def testSpillWithHashCollisions(): Unit = {
    val size = 1000
    sc = newSparkContextWithForceSpillSize(confWithoutKryo, size / 2)
    val context = TestUtil.newTaskContext(sc.conf)

    def createCombiner(i: String): ArrayBuffer[String] = ArrayBuffer[String](i)

    def mergeValue(buffer: ArrayBuffer[String], i: String): ArrayBuffer[String] =
      buffer += i

    def mergeCombiners(
        buffer1: ArrayBuffer[String],
        buffer2: ArrayBuffer[String]): ArrayBuffer[String] = buffer1 ++= buffer2

    val agg = new Aggregator[String, String, ArrayBuffer[String]](
      createCombiner, mergeValue, mergeCombiners)

    val testSorter = new SplashSorter[String, String, ArrayBuffer[String]](
      context, Some(agg), None, None)

    val collisionPairs = Seq(
      ("Aa", "BB"), // 2112
      ("to", "v1"), // 3707
      ("variants", "gelato"), // -1249574770
      ("Teheran", "Siblings"), // 231609873
      ("misused", "horsemints"), // 1069518484
      ("isohel", "epistolaries"), // -1179291542
      ("righto", "buzzards"), // -931102253
      ("hierarch", "crinolines"), // -1732884796
      ("inwork", "hypercatalexes"), // -1183663690
      ("wainages", "presentencing"), // 240183619
      ("trichothecenes", "locular"), // 339006536
      ("pomatoes", "eructation") // 568647356
    )

    collisionPairs.foreach { case (w1, w2) =>
      assertThat(w1.hashCode) isEqualTo w2.hashCode
    }

    val toInsert = (1 to size)
        .map(_.toString)
        .map(s => (s, s)) ++ collisionPairs ++ collisionPairs.map(_.swap)

    testSorter.insertAll(toInsert.iterator)
    assertThat(testSorter.numSpills) isGreaterThan 0

    val collisionMap = (collisionPairs ++ collisionPairs.map(_.swap)).toMap

    var count = 0
    val it = testSorter.iterator
    while (it.hasNext) {
      val kv = it.next()
      val expected = ArrayBuffer[String](collisionMap.getOrElse(kv._1, kv._1))
      assertThat(kv._2) isEqualTo expected
      count += 1
    }
    assertThat(count) isEqualTo size + collisionPairs.size * 2
  }

  def testWithManyHashCollisions(): Unit = {
    val size = 1000
    sc = newSparkContextWithForceSpillSize(confWithoutKryo, size / 2)
    val context = TestUtil.newTaskContext(sc.conf)
    val agg = new Aggregator[FixedHash, Int, Int](_ => 1, _ + _, _ + _)

    val testSorter = new SplashSorter[FixedHash, Int, Int](
      context, Some(agg), None, None)
    val toInsert = for (_ <- 1 to 10; j <- 1 to size) yield (FixedHash(j, j % 2), 1)
    testSorter.insertAll(toInsert.iterator)
    assertThat(testSorter.numSpills) isGreaterThan 0
    val it = testSorter.iterator
    var count = 0
    while (it.hasNext) {
      val Product2(_, i) = it.next()
      assertThat(i) isEqualTo 10
      count += 1
    }
    assertThat(count) isEqualTo size
    testSorter.stop()
  }

  def testSpillWithHashCollisionsUsingIntMaxValueKey(): Unit = {
    val size = 1000
    sc = newSparkContextWithForceSpillSize(confWithoutKryo, size / 2)
    val context = TestUtil.newTaskContext(sc.conf)

    def createCombiner(i: Int): ArrayBuffer[Int] = ArrayBuffer[Int](i)

    def mergeValue(buffer: ArrayBuffer[Int], i: Int): ArrayBuffer[Int] = buffer += i

    def mergeCombiners(buf1: ArrayBuffer[Int], buf2: ArrayBuffer[Int]): ArrayBuffer[Int] = {
      buf1 ++= buf2
    }

    val agg = new Aggregator[Int, Int, ArrayBuffer[Int]](createCombiner, mergeValue, mergeCombiners)

    val testSorter = new SplashSorter[Int, Int, ArrayBuffer[Int]](
      context, Some(agg), None, None)
    testSorter.insertAll(
      (1 to size).iterator.map(i => (i, i)) ++ Iterator((Int.MaxValue, Int.MaxValue)))
    assertThat(testSorter.numSpills) isGreaterThan 0
    val it = testSorter.iterator
    // Should not throw NoSuchElementException
    while (it.hasNext) it.next()
  }

  def testSpillWithNullKeysAndValues(): Unit = {
    val size = 1000
    sc = newSparkContextWithForceSpillSize(confWithoutKryo, size / 2)
    val context = TestUtil.newTaskContext(sc.conf)

    def createCombiner(i: String): ArrayBuffer[String] = ArrayBuffer[String](i)

    def mergeValue(buffer: ArrayBuffer[String], i: String): ArrayBuffer[String] = buffer += i

    def mergeCombiners(buf1: ArrayBuffer[String], buf2: ArrayBuffer[String]): ArrayBuffer[String] = {
      buf1 ++= buf2
    }

    val agg = new Aggregator[String, String, ArrayBuffer[String]](createCombiner, mergeValue, mergeCombiners)

    val testSorter = new SplashSorter[String, String, ArrayBuffer[String]](
      context, Some(agg), None, None)
    testSorter.insertAll((1 to size).iterator.map(i => (i.toString, i.toString)) ++ Iterator(
      (null.asInstanceOf[String], "1"),
      ("1", null.asInstanceOf[String]),
      (null.asInstanceOf[String], null.asInstanceOf[String])
    ))
    assertThat(testSorter.numSpills) isGreaterThan 0
    val it = testSorter.iterator
    // Should not throw NullPointerException
    while (it.hasNext) it.next()
  }

  def testSortUpdatesPeakExecutionMemory(): Unit = {
    val size = 1000
    sc = newSparkContextWithForceSpillSize(confWithoutKryo, size)
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

  def testZeroPartitionLength(): Unit = {
    val size = 500
    sc = newSparkContextWithForceSpillSize(confWithoutKryo, size / 2)
    val actual = sc.parallelize((1 to size).map(FixedHash(_, 10)), 4)
        .keyBy(f => f.hashCode())
        .reduceByKey((v1, v2) => FixedHash(v1.v + v2.v, v1.h))
        .collect().toSet
    val expected = Set((10, FixedHash(125250, 10)))
    assertThat(actual) isEqualTo expected
  }
}

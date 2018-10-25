/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package org.apache.spark.shuffle.memverge

import com.memverge.mvfs.dmo.DMOFile
import org.apache.spark.{SparkContext, TaskContext}
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations._

import scala.collection.mutable.ArrayBuffer

@Test(groups = Array("UnitTest", "IntegrationTest"))
class DMOAppendOnlyMapTest {
  private def createCombiner[T](i: T) = ArrayBuffer[T](i)

  private def mergeValue[T](buffer: ArrayBuffer[T], i: T): ArrayBuffer[T] = buffer += i

  private def mergeCombiners[T](buf1: ArrayBuffer[T], buf2: ArrayBuffer[T]): ArrayBuffer[T] =
    buf1 ++= buf2

  private val rddSize = 1000

  private var sc: SparkContext = _
  private var context: TaskContext = _

  @BeforeClass
  def beforeClass(): Unit = {
    val conf = TestUtil.newSparkConf()
        .set("spark.shuffle.spill.numElementsForceSpillThreshold", (rddSize / 4).toString)
        .set("spark.io.encryption.enabled", "true")
    sc = TestUtil.newSparkContext(conf)
  }

  @BeforeMethod
  def beforeMethod(): Unit = {
    context = TestUtil.newTaskContext(sc.conf)
  }

  @AfterMethod
  def afterMethod(): Unit = {
    DMOAppendOnlyMap.spilledCount = 0
  }

  @AfterClass
  def afterClass(): Unit = {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    DMOFile.safeReset()
  }

  private def createDMOMap[T] = {
    new DMOAppendOnlyMap[T, T, ArrayBuffer[T]](
      createCombiner[T], mergeValue[T], mergeCombiners[T], context = context)
  }

  def testSingleInsert(): Unit = {
    val map = createDMOMap[Int]
    map.insert(1, 10)
    val it = map.iterator
    assertThat(it.hasNext) isTrue()
    val kv = it.next()
    assertThat(kv._1) isEqualTo 1
    assertThat(kv._2) isEqualTo ArrayBuffer[Int](10)
    assertThat(it.hasNext) isFalse()
  }

  def testMultipleInsert(): Unit = {
    val map = createDMOMap[Int]
    map.insert(1, 10)
    map.insert(2, 20)
    map.insert(3, 30)
    val it = map.iterator
    assertThat(it.hasNext) isTrue()
    assertThat(it.toSet) isEqualTo Set(
      (1, ArrayBuffer(10)),
      (2, ArrayBuffer(20)),
      (3, ArrayBuffer(30)))
  }

  def testInsertWithCollision(): Unit = {
    val map = createDMOMap[Int]

    map.insertAll(Seq(
      (1, 10),
      (2, 20),
      (3, 30),
      (1, 100),
      (2, 200),
      (1, 1000)))
    val it = map.iterator
    assertThat(it.hasNext) isTrue()
    val result = it.toSet[(Int, ArrayBuffer[Int])].map(kv => (kv._1, kv._2.toSet))
    assertThat(result) isEqualTo Set(
      (1, Set(10, 100, 1000)),
      (2, Set(20, 200)),
      (3, Set(30)))
  }

  def testOrdering(): Unit = {
    val map1 = createDMOMap[Int]
    map1.insert(1, 10)
    map1.insert(2, 20)
    map1.insert(3, 30)

    val map2 = createDMOMap[Int]
    map2.insert(2, 20)
    map2.insert(3, 30)
    map2.insert(1, 10)

    val map3 = createDMOMap[Int]
    map3.insert(3, 30)
    map3.insert(1, 10)
    map3.insert(2, 20)

    val it1 = map1.iterator
    val it2 = map2.iterator
    val it3 = map3.iterator

    while (it1.hasNext) {
      val kv1 = it1.next()
      val kv2 = it2.next()
      val kv3 = it3.next()

      assertThat(kv1._1) isEqualTo kv2._1
      assertThat(kv1._1) isEqualTo kv3._1
      assertThat(kv1._2) isEqualTo kv2._2
      assertThat(kv1._2) isEqualTo kv3._2
    }
  }

  def testNullKeysAndValues(): Unit = {
    val map = createDMOMap[Int]
    val nullInt = null.asInstanceOf[Int]
    map.insert(1, 5)
    map.insert(2, 6)
    map.insert(3, 7)
    map.insert(4, nullInt)
    map.insert(nullInt, 8)
    map.insert(nullInt, nullInt)
    val result = map.iterator
        .toSet[(Int, ArrayBuffer[Int])]
        .map(kv => (kv._1, kv._2.sorted))
    assertThat(result) isEqualTo Set(
      (1, Seq(5)),
      (2, Seq(6)),
      (3, Seq(7)),
      (4, Seq(nullInt)),
      (nullInt, Seq(nullInt, 8)))
  }

  def testSimpleAggregator(): Unit = {
    val rdd = sc.parallelize(1 to 10).map(i => (i % 2, 1))
    val result1 = rdd.reduceByKey(_ + _).collect().toSet
    assertThat(result1) isEqualTo Set((0, 5), (1, 5))

    val result2 = rdd.groupByKey().collect().map(x => (x._1, x._2.toList)).toSet
    assertThat(result2) isEqualTo Set(
      (0, List(1, 1, 1, 1, 1)),
      (1, List(1, 1, 1, 1, 1)))
  }

  def testSimpleCogroup(): Unit = {
    val rdd1 = sc.parallelize(1 to 4).map(i => (i, i))
    val rdd2 = sc.parallelize(1 to 4).map(i => (i % 2, i))
    val result = rdd1.cogroup(rdd2).collect()

    result.foreach { case (i, (seq1, seq2)) =>
      i match {
        case 0 =>
          assertThat(seq1.toSet) isEqualTo Set()
          assertThat(seq2.toSet) isEqualTo Set(2, 4)
        case 1 =>
          assertThat(seq1.toSet) isEqualTo Set(1)
          assertThat(seq2.toSet) isEqualTo Set(1, 3)
        case 2 =>
          assertThat(seq1.toSet) isEqualTo Set(2)
          assertThat(seq2.toSet) isEqualTo Set()
        case 3 =>
          assertThat(seq1.toSet) isEqualTo Set(3)
          assertThat(seq2.toSet) isEqualTo Set()
        case 4 =>
          assertThat(seq1.toSet) isEqualTo Set(4)
          assertThat(seq2.toSet) isEqualTo Set()
      }
    }
  }

  def testReduceByKeySpilling(): Unit = {
    assertSpilled("reduceByKey") {
      val result = sc.parallelize(0 until rddSize)
          .map { i => (i / 2, i) }
          .reduceByKey(math.max)
          .collect()
      assertThat(result.length) isEqualTo rddSize / 2
      result.foreach { case (k, v) =>
        val expected = k * 2 + 1
        assertThat(v) isEqualTo expected
      }
    }
  }

  def testGroupByKeySpilling(): Unit = {
    assertSpilled("groupByKey") {
      val result = sc.parallelize(0 until rddSize)
          .map { i => (i / 2, i) }
          .groupByKey()
          .collect()
      assertThat(result.length) isEqualTo rddSize / 2
      result.foreach { case (i, seq) =>
        val actual = seq.toSet
        val expected = Set(i * 2, i * 2 + 1)
        assertThat(actual) isEqualTo expected
      }
    }
  }

  def testCogroupSpilling(): Unit = {
    assertNotSpilled("cogroup") {
      val rdd1 = sc.parallelize(0 until rddSize).map { i => (i / 2, i) }
      val rdd2 = sc.parallelize(0 until rddSize).map { i => (i / 2, i) }
      val result = rdd1.cogroup(rdd2).collect()
      assertThat(result.length) isEqualTo rddSize / 2
      result.foreach { case (i, (seq1, seq2)) =>
        val actual1 = seq1.toSet
        val actual2 = seq2.toSet
        val expected = Set(i * 2, i * 2 + 1)
        assertThat(actual1) isEqualTo expected
        assertThat(actual2) isEqualTo expected
      }
    }
  }

  private def assertSpilled[T](identifier: String)(body: => T): Unit = {
    body
    assertThat(DMOAppendOnlyMap.spilledCount).isGreaterThan(0)
  }

  private def assertNotSpilled[T](identifier: String)(body: => T): Unit = {
    body
    assertThat(DMOAppendOnlyMap.spilledCount).isEqualTo(0)
  }

  def testForceSpill(): Unit = {
    val map = createDMOMap[String]
    val consumer = createDMOMap[String]
    map.insertAll((1 to rddSize).iterator.map(_.toString).map(i => (i, i)))
    assertThat(map.spill(10000, consumer)) isGreaterThan 0L
  }

  def testSpillingWithHashCollisions(): Unit = {
    val map = createDMOMap[String]

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
      // String.hashCode is documented to use a specific algorithm, but check just in case
      assertThat(w1.hashCode) isEqualTo w2.hashCode
    }

    map.insertAll((1 to rddSize).iterator.map(_.toString).map(i => (i, i)))
    collisionPairs.foreach { case (w1, w2) =>
      map.insert(w1, w2)
      map.insert(w2, w1)
    }
    assertThat(DMOAppendOnlyMap.spilledCount) isGreaterThan 0

    // A map of collision pairs in both directions
    val collisionPairsMap = (collisionPairs ++ collisionPairs.map(_.swap)).toMap

    // Avoid map.size or map.iterator.length because this destructively sorts the underlying map
    var count = 0

    val it = map.iterator
    while (it.hasNext) {
      val kv = it.next()
      val expectedValue = ArrayBuffer[String](collisionPairsMap.getOrElse(kv._1, kv._1))
      assert(kv._2.equals(expectedValue))
      count += 1
    }
    assertThat(count) isEqualTo rddSize + collisionPairs.size * 2
  }

  def testSpillingWithManyHashCollisions(): Unit = {
    val map =
      new DMOAppendOnlyMap[FixedHash, Int, Int](_ => 1, _ + _, _ + _, context = context)

    // Insert 10 copies each of lots of objects whose hash codes are either 0 or 1. This causes
    // problems if the map fails to group together the objects with the same code (SPARK-2043).
    for (_ <- 1 to 10) {
      for (j <- 1 to rddSize) {
        map.insert(FixedHash(j, j % 2), 1)
      }
    }

    assertThat(DMOAppendOnlyMap.spilledCount) isGreaterThan 0

    val it = map.iterator
    var count = 0
    while (it.hasNext) {
      val kv = it.next()
      assertThat(kv._2) isEqualTo 10
      count += 1
    }

    assertThat(count) isEqualTo rddSize
  }

  def testSpillingWithHashCollisionsUsingIntMaxValue(): Unit = {
    val map = createDMOMap[Int]

    (1 to rddSize).foreach { i => map.insert(i, i) }
    map.insert(Int.MaxValue, Int.MaxValue)
    assert(map.numSpills > 0, "map did not spill")

    val it = map.iterator
    while (it.hasNext) {
      // Should not throw NoSuchElementException
      it.next()
    }
  }

  def testSpillingWithNullKeysAndValues(): Unit = {
    val map = createDMOMap[Int]

    map.insertAll((1 to rddSize).iterator.map(i => (i, i)))
    map.insert(null.asInstanceOf[Int], 1)
    map.insert(1, null.asInstanceOf[Int])
    map.insert(null.asInstanceOf[Int], null.asInstanceOf[Int])
    assert(map.numSpills > 0, "map did not spill")

    val it = map.iterator
    while (it.hasNext) {
      // Should not throw NullPointerException
      it.next()
    }
  }
}

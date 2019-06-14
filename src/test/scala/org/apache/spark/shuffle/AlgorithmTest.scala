/*
 * Modifications copyright (C) 2019 MemVerge Inc.
 *
 * Extract the logic related to algorithms from the shuffle manager.
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

import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.Test

@Test(groups = Array("UnitTest", "IntegrationTest"))
class AlgorithmTest {
  def testMergeSort(): Unit = {
    val size = 100
    val partitionSize = 10
    val partitions = (0 until partitionSize).map(n => {
      (0 to size).filter(_ % partitionSize == n)
    })
    val sorted = Algorithm.mergeSort(
      partitions.map(_.iterator),
      new Ordering[Int] {
        override def compare(x: Int, y: Int): Int = x - y
      })

    assertThat(sorted.toList).isEqualTo(0 to size)
  }

  def testMergeSortWithDupAtBeginning(): Unit = {
    val i1 = List(1, 3, 5)
    val i2 = List(1, 4)
    val sorted = Algorithm.mergeSort(
      Seq(i1.iterator, i2.iterator),
      new Ordering[Int] {
        override def compare(x: Int, y: Int): Int = x - y
      })
    assertThat(sorted.toList).isEqualTo(List(1, 1, 3, 4, 5))
  }

  def testMergeSortWithDupValue(): Unit = {
    val i1 = List(1, 3, 5, 7)
    val i2 = List(2, 3)
    val i3 = List(7)
    val i4 = List(5)
    val sorted = Algorithm.mergeSort(
      Seq(i1.iterator, i2.iterator, i3.iterator, i4.iterator),
      new Ordering[Int] {
        override def compare(x: Int, y: Int): Int = x - y
      })
    assertThat(sorted.toList).isEqualTo(List(1, 2, 3, 3, 5, 5, 7, 7))
  }

  def testMergeSortWithSameHashCode(): Unit = {
    val i1 = List("Aa", "BB", "to")
    val i2 = List( "v1")
    val sorted = Algorithm.mergeSort(
      Seq(i1.iterator, i2.iterator),
      new Ordering[String] {
        override def compare(x: String, y: String): Int = x.hashCode - y.hashCode
      })
    assertThat(sorted.toList).isEqualTo(List("Aa", "BB", "to", "v1"))
  }

  @Test(enabled = false)
  def testMergeSortPerformance(): Unit = {
    val size = 10000000
    val partitionSize = 500
    val partitions = (0 until partitionSize).map(n => {
      (0 to size).filter(_ % partitionSize == n)
    })

    TestUtil.time {
      val sorted = Algorithm.mergeSort(
        partitions.map(_.iterator),
        new Ordering[Int] {
          override def compare(x: Int, y: Int): Int = x - y
        })

      assertThat(sorted.next) isEqualTo 0
      assertThat(sorted.toStream.last) isEqualTo size
    }(50)
  }
}

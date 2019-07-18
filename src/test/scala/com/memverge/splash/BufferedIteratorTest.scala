/*
 * Copyright (C) 2018 MemVerge Inc.
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
package com.memverge.splash

import org.assertj.core.api.Assertions.{assertThat, assertThatExceptionOfType}
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.testng.annotations.Test

@Test(groups = Array("UnitTest", "IntegrationTest"))
class BufferedIteratorTest {
  def testFillSmallerThanBuffer(): Unit = {
    val buffer = BufferedIterator(IntIterator(5), 10)
    buffer.fill()
    assertThat(buffer.bufferLastOpt().getOrElse(-1)).isEqualTo(4)
  }

  def testFillLargerThanBuffer(): Unit = {
    val buffer = BufferedIterator(IntIterator(20), 10)
    buffer.fill()
    assertThat(buffer.bufferLastOpt().getOrElse(-1)).isEqualTo(9)
  }

  def testNextWithoutReFill(): Unit = {
    val buffer = BufferedIterator(IntIterator(10), 5)
    assertThat(buffer.hasNext).isTrue
    assertThat(buffer.next()).isEqualTo(0)
    assertThat(buffer.hasNext).isTrue
    assertThat(buffer.next()).isEqualTo(1)
  }

  def testNextWithReFill(): Unit = {
    val buffer = BufferedIterator(IntIterator(5), 2)
    (0 to 4).foreach(i => {
      assertThat(buffer.hasNext).isTrue
      assertThat(buffer.next()).isEqualTo(i)
    })
    verifyNoElementAvailable(buffer)
  }

  private def verifyNoElementAvailable(buffer: BufferedIterator[Int]):Unit = {
    assertThat(buffer.hasNext).isFalse
    assertThatExceptionOfType(classOf[NoSuchElementException])
        .isThrownBy(new ThrowingCallable {
          override def call(): Unit = buffer.next()
        })
  }

  def testNextOnEmptyCollection(): Unit = {
    val buffer = BufferedIterator(IntIterator(0), 10)
    verifyNoElementAvailable(buffer)
  }

  def testNextWithMultipleReFills(): Unit = {
    val buffer = BufferedIterator(IntIterator(50), 2)
    (0 to 49).foreach(i => {
      assertThat(buffer.hasNext).isTrue
      assertThat(buffer.next()).isEqualTo(i)
    })
    verifyNoElementAvailable(buffer)
  }

  def testNextWithSingleFill(): Unit = {
    val buffer = BufferedIterator(IntIterator(2), 5)
    (0 to 1).foreach(i => {
      assertThat(buffer.hasNext).isTrue
      assertThat(buffer.next()).isEqualTo(i)
    })
    verifyNoElementAvailable(buffer)
  }

  def testLastOptOnEmptyCollection(): Unit = {
    val buffer = BufferedIterator(IntIterator(0))
    assertThat(buffer.bufferLastOpt()).isEqualTo(None)
  }
}


case class IntIterator(limit: Int) extends Iterator[Int]{
  private var value = 0

  override def next(): Int = {
    val ret = value
    value += 1
    ret
  }

  override def hasNext: Boolean = value < limit
}
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

import java.util.NoSuchElementException

import scala.reflect.ClassTag


case class BufferedIterator[T](
    iterator: Iterator[T],
    bufferSize: Int = 512)(implicit m: ClassTag[T]) extends Iterator[T] {
  private var writeIndex: Int = 0
  private val buffer = new Array[T](bufferSize)
  private var readIndex: Int = 0

  fill()

  override def next(): T = {
    if (readIndex == writeIndex && writeIndex < bufferSize) {
      throw new NoSuchElementException("end of iterator")
    }
    val obj = buffer(readIndex)
    readIndex += 1
    if (readIndex == writeIndex) {
      if (writeIndex == bufferSize) {
        readIndex = 0
        writeIndex = 0
        fill()
      }
    }
    obj
  }

  override def hasNext: Boolean = readIndex < writeIndex

  def fill(): Unit = {
    while (iterator.hasNext && writeIndex < bufferSize) {
      val obj = iterator.next()
      if (obj != null) {
        buffer(writeIndex) = obj
        writeIndex += 1
      }
    }
  }

  def bufferLastOpt(): Option[T] = {
    if (writeIndex == 0) {
      None
    } else {
      Some(buffer(writeIndex - 1))
    }
  }
}


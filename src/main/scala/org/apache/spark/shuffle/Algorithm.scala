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

import java.util

import scala.collection.mutable


object Algorithm {
  /**
   * Put the head of the iterators into a treemap so that we could retrieve
   * the sorted header in an efficient way.
   *
   * The original implementation is using a priority queue to sort the iterators.
   * This is not efficient especially when the partition size is large.
   *
   * The drawback of using a map tree is that it overrides duplicated values.
   * We use an extra priority queue to deal with those values.
   *
   * If the key already exists in the treemap, the value is buffered in a
   * priority queue.  It will be added back to the treemap when the duplicated
   * key is removed from the tree map.
   */
  def mergeSort[T](
      iterators: Seq[Iterator[T]],
      comparator: util.Comparator[T]): Iterator[T] = {
    type BIterator = BufferedIterator[T]

    val heap = new mutable.PriorityQueue[BIterator]()(new Ordering[BIterator] {
      // Use the reverse of comparator.compare because PriorityQueue dequeues the max
      override def compare(x: BIterator, y: BIterator): Int =
        -comparator.compare(x.head, y.head)
    })

    val treeMap = new util.TreeMap[T, BIterator](comparator)

    def put(iterator: BIterator): Unit = {
      val prev = treeMap.put(iterator.head, iterator)
      if (prev != null) {
        heap.enqueue(prev)
      }
    }

    iterators.filter(_.hasNext).map(_.buffered).foreach(put)

    new Iterator[T] {
      override def hasNext: Boolean = !treeMap.isEmpty

      override def next(): T = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val entry = treeMap.firstEntry()
        val value = entry.getValue
        // do not use map key directly because of hash collision
        val key = value.head

        treeMap.remove(key)
        if (heap.nonEmpty && comparator.compare(heap.head.head, key) == 0) {
          val iteratorWithSameKey = heap.dequeue()
          treeMap.put(iteratorWithSameKey.head, iteratorWithSameKey)
        } else if (treeMap.isEmpty && heap.nonEmpty) {
          val iterator = heap.dequeue()
          treeMap.put(iterator.head, iterator)
        }

        value.next()
        if (value.hasNext) put(value)
        key
      }
    }
  }
}

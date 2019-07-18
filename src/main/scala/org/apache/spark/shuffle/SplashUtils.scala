/*
 * Modifications copyright (C) 2018 MemVerge Inc.
 *
 * Extract some inner classes to package visible utility classes.
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

import java.util.Comparator

import org.apache.spark.internal.Logging

import scala.util.control.NonFatal

private[spark] object SplashUtils extends Logging {
  def withResources[T <: AutoCloseable, V](r: => T)(f: T => V): V = {
    val resource: T = r
    require(resource != null, "resource is null")
    var exception: Throwable = null
    try {
      f(resource)
    } catch {
      case NonFatal(e) =>
        exception = e
        throw e
      case e: Throwable =>
        logError("fatal error received.", e)
        throw e
    } finally {
      closeAndAddSuppressed(exception, resource)
    }
  }

  private def closeAndAddSuppressed(e: Throwable,
      resource: AutoCloseable): Unit = {
    if (e != null) {
      try {
        resource.close()
      } catch {
        case NonFatal(suppressed) =>
          e.addSuppressed(suppressed)
      }
    } else {
      resource.close()
    }
  }

  /**
   * Return the hash code of the given object. If the object is null,
   * return a special hash code.
   */
  def hash[T](obj: T): Int = {
    if (obj == null) 0 else obj.hashCode()
  }
}


/**
 * A comparator which sorts arbitrary keys based on their hash codes.
 */
class SplashHashComparator[K] extends Comparator[K] {
  def compare(key1: K, key2: K): Int = {
    val hash1 = SplashUtils.hash(key1)
    val hash2 = SplashUtils.hash(key2)
    if (hash1 < hash2) -1 else if (hash1 == hash2) 0 else 1
  }
}


class SplashSpillableIterator[T](var upstream: Iterator[T],
    val spillInMemoryIterator: Iterator[T] => SpilledFile,
    val getNextUpstream: SpilledFile => Iterator[T])
    extends Iterator[T] with Logging {
  private val spillLock = new Object
  private var spilledFileOpt: Option[SpilledFile] = None
  private var cur: T = readNext()

  def spill(): Option[SpilledFile] = spillLock.synchronized {
    spilledFileOpt match {
      case Some(_) =>
        // has spilled, return None
        None
      case None =>
        // never spilled, now spilling
        val spilledFile = spillInMemoryIterator(upstream)
        spilledFileOpt = Some(spilledFile)
        spilledFileOpt
    }
  }

  def readNext(): T = spillLock.synchronized {
    spilledFileOpt match {
      case Some(spilledFile) =>
        upstream = getNextUpstream(spilledFile)
        spilledFileOpt = None
      case None =>
      // do nothing
    }
    if (upstream.hasNext) {
      upstream.next()
    } else {
      null.asInstanceOf[T]
    }
  }

  override def hasNext: Boolean = cur != null

  override def next(): T = {
    val ret = cur
    cur = readNext()
    ret
  }
}

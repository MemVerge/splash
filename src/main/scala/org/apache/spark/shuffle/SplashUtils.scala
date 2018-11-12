/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle

import java.util.Comparator

import org.apache.spark.internal.Logging

import scala.util.control.NonFatal

private[spark] object SplashUtils {
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
  private var nextUpstream: Iterator[T] = _
  private var cur: T = readNext()
  private var spilledFileOpt: Option[SpilledFile] = None

  def spill(): Option[SpilledFile] = spillLock.synchronized {
    spilledFileOpt match {
      case Some(_) =>
        // has spilled, return None
        None
      case None =>
        // never spilled, now spilling
        val spilledFile = spillInMemoryIterator(upstream)
        val shuffleTmpFile = spilledFile.file
        nextUpstream = getNextUpstream(spilledFile)
        logInfo(s"spilling in-memory " +
            s"data structure to storage ${shuffleTmpFile.getId} " +
            s"size ${shuffleTmpFile.getSize}.")
        spilledFileOpt = Some(spilledFile)
        spilledFileOpt
    }
  }

  def readNext(): T = spillLock.synchronized {
    if (nextUpstream != null) {
      upstream = nextUpstream
      nextUpstream = null
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

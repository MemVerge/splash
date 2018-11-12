/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle

import java.util.Properties
import java.util.concurrent.Semaphore

import javax.annotation.concurrent.GuardedBy
import org.apache.spark.InternalAccumulator.PEAK_EXECUTION_MEMORY
import org.apache.spark._
import org.apache.spark.memory.{MemoryManager, MemoryMode, TaskMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler._
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.BlockId
import org.assertj.core.api.Assertions.assertThat

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[spark] object TestUtil {
  private def newMemoryManager(conf: SparkConf): MemoryManager = {
    val onHeapStorageMemory = 100 * 1024 * 1024
    val onHeapExeMemory = 200 * 1024 * 1024
    val cores = 4
    new MemoryManager(
      conf, cores, onHeapStorageMemory, onHeapExeMemory) {
      /** @inheritdoc*/
      override def maxOnHeapStorageMemory: Long = onHeapStorageMemory

      /** @inheritdoc*/
      override def maxOffHeapStorageMemory: Long = onHeapStorageMemory * 2

      /** @inheritdoc*/
      override def acquireStorageMemory(
          blockId: BlockId,
          numBytes: Long,
          memoryMode: MemoryMode): Boolean = true

      /** @inheritdoc*/
      override def acquireUnrollMemory(
          blockId: BlockId,
          numBytes: Long,
          memoryMode: MemoryMode): Boolean = true

      def acquireExecutionMemory(
          numBytes: Long,
          taskAttemptId: Long,
          memoryMode: MemoryMode): Long = numBytes
    }
  }

  private def newMetricsSystem(conf: SparkConf): MetricsSystem = {
    MetricsSystem.createMetricsSystem(
      "testMetricsSystem",
      conf,
      new SecurityManager(conf))
  }

  def newSparkConf(threads: Int = 1): SparkConf = new SparkConf().setAppName("testApp")
      .setMaster(s"local[$threads]")
      .set("spark.ui.enabled", "false")
      .set("spark.shuffle.manager", classOf[SplashShuffleManager].getName)
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.shuffle.spill.batchSize", "10")
      .set("spark.shuffle.spill.initialMemoryThreshold", "512")

  def newBaseShuffleConf: SparkConf = newSparkConf()
      .set("spark.shuffle.mvfs.useBaseShuffle", "true")

  def newTaskContext(conf: SparkConf): TaskContext = {
    val taskMemoryManager = new TaskMemoryManager(newMemoryManager(conf), 0)
    new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      taskAttemptId = 0,
      attemptNumber = 0,
      taskMemoryManager = taskMemoryManager,
      localProperties = new Properties,
      metricsSystem = newMetricsSystem(conf))
  }

  def newDMOSpillSorter[K, V, C](
      aggregator: Option[Aggregator[K, V, C]] = None,
      partitioner: Option[Partitioner] = None,
      ordering: Option[Ordering[K]] = None,
      conf: SparkConf = newBaseShuffleConf): SplashSorter[K, V, C] = {
    val context = newTaskContext(conf)
    new SplashSorter[K, V, C](
      context,
      aggregator,
      partitioner,
      ordering,
      SplashSerializer(new JavaSerializer(conf))
    )
  }

  def newSparkContext(conf: SparkConf): SparkContext = {
    new SparkContext(conf)
  }

  def sumAgg: Aggregator[Int, Int, Int] = {
    new Aggregator[Int, Int, Int](i => i, (i, j) => i + j, (i, j) => i + j)
  }

  def verifyPeakExecutionMemorySet(
      sc: SparkContext,
      testName: String)(testBody: => Unit): Unit = {
    val listener = new SaveInfoListener
    sc.addSparkListener(listener)
    testBody
    // wait until all events have been processed before proceeding to assert things
    sc.listenerBus.waitUntilEmpty(10 * 1000)
    val accums = listener.getCompletedStageInfos.flatMap(_.accumulables.values)
    val isSet = accums.exists { a =>
      a.name.contains(PEAK_EXECUTION_MEMORY) && a.value.exists(_.asInstanceOf[Long] > 0L)
    }
    if (!isSet) {
      throw new SparkException(s"peak execution memory accumulator not set in '$testName'")
    }
  }

  def assertNotSpilled[T](sc: SparkContext)(body: => T): Unit = {
    val spillListener = new SpillListener
    sc.addSparkListener(spillListener)
    body
    assertThat(spillListener.numSpilledStages) isEqualTo 0
    sc.removeSparkListener(spillListener)
  }

  def assertSpilled[T](sc: SparkContext)(body: => T): Unit = {
    val spillListener = new SpillListener
    sc.addSparkListener(spillListener)
    body
    assertThat(spillListener.numSpilledStages) isGreaterThan 0
    sc.removeSparkListener(spillListener)
  }

  def IntentionalFailure(): Exception = {
    new SparkException("INTENTIONAL failure, ignore this.")
  }
}

/**
  * A dummy class that always returns the same hash code, to easily test hash collisions
  */
case class FixedHash(v: Int, h: Int) extends Serializable {
  override def hashCode(): Int = h

  override def equals(other: Any): Boolean = other match {
    case that: FixedHash => v == that.v && h == that.h
    case _ => false
  }

  override def toString: String = s"FixedHash[v: $v, h: $h]"
}

/**
  * A simple listener that keeps track of the TaskInfos and StageInfos of all completed jobs.
  */
private class SaveInfoListener extends SparkListener {
  type StageId = Int
  type StageAttemptId = Int

  private val completedStageInfos = new ArrayBuffer[StageInfo]
  private val completedTaskInfos =
    new mutable.HashMap[(StageId, StageAttemptId), ArrayBuffer[TaskInfo]]

  // Callback to call when a job completes. Parameter is job ID.
  @GuardedBy("this")
  private var jobCompletionCallback: () => Unit = _
  private val jobCompletionSem = new Semaphore(0)
  private var exception: Throwable = _

  def getCompletedStageInfos: Seq[StageInfo] = completedStageInfos.toArray.toSeq

  def getCompletedTaskInfos: Seq[TaskInfo] = completedTaskInfos.values.flatten.toSeq

  def getCompletedTaskInfos(stageId: StageId, stageAttemptId: StageAttemptId): Seq[TaskInfo] =
    completedTaskInfos.getOrElse((stageId, stageAttemptId), Seq.empty[TaskInfo])

  /**
    * If `jobCompletionCallback` is set, block until the next call has finished.
    * If the callback failed with an exception, throw it.
    */
  def awaitNextJobCompletion(): Unit = {
    if (jobCompletionCallback != null) {
      jobCompletionSem.acquire()
      if (exception != null) {
        throw exception
      }
    }
  }

  /**
    * Register a callback to be called on job end.
    * A call to this should be followed by [[awaitNextJobCompletion]].
    */
  def registerJobCompletionCallback(callback: () => Unit): Unit = {
    jobCompletionCallback = callback
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    if (jobCompletionCallback != null) {
      try {
        jobCompletionCallback()
      } catch {
        // Store any exception thrown here so we can throw them later in the main thread.
        // Otherwise, if `jobCompletionCallback` threw something it wouldn't fail the test.
        case NonFatal(e) => exception = e
      } finally {
        jobCompletionSem.release()
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    completedStageInfos += stageCompleted.stageInfo
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    completedTaskInfos.getOrElseUpdate(
      (taskEnd.stageId, taskEnd.stageAttemptId), new ArrayBuffer[TaskInfo]) += taskEnd.taskInfo
  }
}

case class ConfWithReducerN(conf: SparkConf, numReducer: Int)

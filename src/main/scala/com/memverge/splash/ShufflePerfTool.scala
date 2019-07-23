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

import java.io._
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, Executors}

import org.apache.commons.cli.{DefaultParser, HelpFormatter, MissingOptionException, Options, ParseException, UnrecognizedOptionException}
import org.apache.spark.SparkException
import org.apache.spark.shuffle.{SplashOpts, SplashShuffleBlockResolver, SplashUtils}
import org.apache.spark.storage.{BlockId, ShuffleBlockId}

import scala.util.Random

object ShufflePerfTool {
  private lazy val options = initOptions()

  private[splash] def parse(args: Array[String]): Either[String, ShuffleToolOption] = {
    val className = "DMOShufflePerfTool"
    val parser = new DefaultParser()
    try {
      val cmd = parser.parse(options, args)
      if (cmd.hasOption("h")) {
        new HelpFormatter().printHelp(className, options)
        Left("")
      } else {
        Right(ShuffleToolOption(
          cmd.getOptionValue("f", SplashOpts.storageFactoryName.defaultValueString),
          Integer.parseInt(cmd.getOptionValue("i", "1")),
          Integer.parseInt(cmd.getOptionValue("t", "5")),
          Integer.parseInt(cmd.getOptionValue("m", "10")),
          Integer.parseInt(cmd.getOptionValue("r", "10")),
          Integer.parseInt(cmd.getOptionValue("d", "1024")),
          Integer.parseInt(cmd.getOptionValue("b", "262144")),
          overwrite = cmd.hasOption("o"),
          readOnly = cmd.hasOption("ro")))
      }
    } catch {
      case e@(_: UnrecognizedOptionException | _: MissingOptionException) =>
        Left(e.getMessage)
      case e: NumberFormatException =>
        Left(s"invalid integer received.  ${e.getMessage}")
      case e: ParseException =>
        Left(s"parse error: ${e.getMessage}")
    }
  }

  private def initOptions(): Options = {
    val options = new Options()
    options.addOption("h", "help", false, "display help message")
    options.addOption("f", "factory", true, "Storage factory name")
    options.addOption("i", "shuffleId", true, "shuffle ID")
    options.addOption("t", "tasks", true, "# of concurrent tasks")
    options.addOption("m", "mappers", true, "# of mappers")
    options.addOption("r", "reducers", true, "# of reducers")
    options.addOption("d", "data", true, "# of data blocks")
    options.addOption("b", "blockSize", true, "block/buffer size")
    options.addOption("o", "overwrite", false, "overwrite existing outputs")
    options.addOption("ro", "readOnly", false, "read only, do not perform shuffle write")
    options
  }


  case class ShuffleToolOption(
      factoryName: String,
      shuffleId: Int,
      tasks: Int,
      mappers: Int,
      reducers: Int,
      dataBlocks: Int,
      blockSize: Int,
      readBufferSize: Int = 256 * 1024,
      overwrite: Boolean = false,
      readOnly: Boolean = false) {

    StorageFactoryHolder.setDefaultFactoryName(factoryName)

    private val appId = s"shuffleTest-$shuffleId"
    private val shuffleSize: Long = dataBlocks * blockSize
    private val partitionSize = shuffleSize / reducers
    private val totalShuffleSize = shuffleSize * mappers

    private val bytesWritten = new AtomicLong(0)
    private val bytesRead = new AtomicLong(0)
    private val totalTimeReadIndex = new AtomicLong(0)
    private val totalTimeReadData = new AtomicLong(0)

    @volatile
    private var latestProgress = ""

    private val resolver = new SplashShuffleBlockResolver(appId)

    private def shuffleFolder = resolver.shuffleFolder

    def writeShuffle(): String = {
      if (readOnly) {
        return "Read only specified.  Skip shuffle write.\n" +
            "Assume that the shuffle output files already exist."
      }

      if (overwrite) {
        println(s"overwrite, removing existing shuffle for $appId")
        resolver.cleanup()
      }

      val start = System.nanoTime()
      print(
        s"""|==========================================
            |Writing $mappers shuffle with $tasks threads:""".stripMargin)
      printProgress(0, mappers)
      val pool = Executors.newFixedThreadPool(tasks)
      val done = new CountDownLatch(mappers)
      (0 until mappers).foreach(mapperId => {
        pool.submit(new Runnable {
          override def run(): Unit = {
            try {
              writeDataFile(shuffleId, mapperId)
              writeIndexFile(shuffleId, mapperId)
            } catch {
              case e: Exception =>
                println(s"write ${shuffleId}_$mapperId failed, ${e.getMessage}")
            } finally {
              printProgress((mappers - done.getCount + 1).intValue, mappers)
              done.countDown()
            }
          }
        })
      })
      pool.shutdown()

      done.await()
      print("\n")

      val duration = Duration.ofNanos(System.nanoTime() - start)
      getSummaryString("Write shuffle data", duration)
    }

    private def printProgress(done: Int, total: Int): Unit = {
      // update the progress every 1%
      if (done == total || (done * 100) % total == 0) {
        if (done == 0) latestProgress = ""
        val goBack = Array.fill(latestProgress.length)("\b").mkString("")
        val detail = s"($done/$total)"
        latestProgress = f" ${done * 100 / total}%3d%% $detail%20s"
        print(goBack + latestProgress)
      }
    }

    private def writeIndexFile(shuffleId: Int, mapperId: Int): Unit = {
      val indexFile = resolver.getIndexTmpFile(shuffleId, mapperId)
      val partitionSize = shuffleSize / reducers
      SplashUtils.withResources {
        new DataOutputStream(
          new BufferedOutputStream(
            indexFile.makeOutputStream()))
      } { os =>
        (0 to reducers).foreach { i =>
          os.writeLong(i * partitionSize)
        }
        bytesWritten.addAndGet(8 * (reducers + 1))
      }
      indexFile.commit()
    }

    private def writeDataFile(shuffleId: Int, mapperId: Int): Unit = {
      val dataFile = resolver.getDataTmpFile(shuffleId, mapperId)
      val rand = new Random
      val buffer = new Array[Byte](blockSize)
      SplashUtils.withResources {
        new DataOutputStream(
          new BufferedOutputStream(
            dataFile.makeOutputStream()))
      } { os =>
        (0 until dataBlocks).foreach(_ => {
          rand.nextBytes(buffer)
          os.write(buffer)
          bytesWritten.addAndGet(buffer.length)
        })
      }
      dataFile.commit()
    }

    def readShuffle(): String = {
      val start = System.nanoTime()
      val total = mappers * reducers
      print(
        s"""|==========================================
            |Reading $total partitions with $tasks threads:""".stripMargin)
      printProgress(0, total)
      val pool = Executors.newFixedThreadPool(tasks)
      val done = new CountDownLatch(reducers * mappers)
      (0 until reducers).foreach(r => {
        (0 until mappers).foreach(m => {
          pool.submit(new Runnable {
            override def run(): Unit = {
              try {
                val partitionId = ShuffleBlockId(shuffleId, m, r)
                getPartitionIS(partitionId) match {
                  case Some(is) =>
                    SplashUtils.withResources(is) { is =>
                      val readDataStart = System.nanoTime
                      val toRead = is.available()
                      val buffer = new Array[Byte](readBufferSize)
                      (0 to toRead / buffer.length).foreach { _ =>
                        bytesRead.addAndGet(is.read(buffer))
                      }
                      val delta = System.nanoTime - readDataStart
                      totalTimeReadData.addAndGet(delta)
                    }
                  case None =>
                    throw new SparkException(s"read $partitionId failed.")
                }
              } catch {
                case e: Exception =>
                  println(s"read shuffle_${shuffleId}_${m}_$r failed, ${e.getMessage}")
              } finally {
                printProgress((total - done.getCount + 1).intValue, total)
                done.countDown()
              }
            }
          })
        })
      })
      pool.shutdown()
      done.await()
      print("\n")

      val duration = Duration.ofNanos(System.nanoTime() - start)
      getSummaryString("Read shuffle data", duration)
    }

    private def getSummaryString(op: String, duration: Duration): String = {
      StorageFactoryHolder.onApplicationEnd()
      val readIndexTook = Duration.ofNanos(totalTimeReadIndex.get())
      val readDataTook = Duration.ofNanos(totalTimeReadData.get())
      val bandwidth = totalShuffleSize * 1000 / duration.toMillis
      val scale = toSizeStr(1024L)(_)
      s"""|$op completed in ${duration.toMillis} milliseconds
          |    Reading index file:  ${readIndexTook.toMillis} ms
          |    Reading data file:   ${readDataTook.toMillis} ms
          |    storage factory:     ${resolver.getStorageFactoryClassName}
          |    shuffle folder:      $shuffleFolder
          |    number of mappers:   $mappers
          |    number of reducers:  $reducers
          |    total shuffle size:  ${scale(totalShuffleSize)}B
          |    bytes written:       ${scale(bytesWritten.get())}B
          |    bytes read:          ${scale(bytesRead.get())}B
          |    number of blocks:    $dataBlocks
          |    blocks size:         ${scale(blockSize)}B
          |    partition size:      ${scale(partitionSize)}B
          |    concurrent tasks:    $tasks
          |    bandwidth:           ${scale(bandwidth)}B/s
          |""".stripMargin
    }

    private def getPartitionIS(blockId: ShuffleBlockId): Option[InputStream] = {
      val start = System.nanoTime()
      try {
        val ret = resolver.getBlockData(blockId.asInstanceOf[BlockId]).map(_.is)
        totalTimeReadIndex.addAndGet(System.nanoTime() - start)
        ret
      }
      catch {
        case _: IOException => None
      }
    }
  }

  private[splash] def execute(args: Array[String]): Unit = {
    parse(args) match {
      case Left(errorMsg) =>
        println(errorMsg)
      case Right(option) =>
        StorageFactoryHolder.onApplicationStart()
        println(option.writeShuffle())
        println(option.readShuffle())
        StorageFactoryHolder.onApplicationEnd()
    }
  }

  private def toSizeStr(scale: Long)(size: Long): String = {
    val kbScale: Long = scale
    val mbScale: Long = scale * kbScale
    val gbScale: Long = scale * mbScale
    val tbScale: Long = scale * gbScale
    if (size > tbScale) {
      size / tbScale + "T"
    } else if (size > gbScale) {
      size / gbScale + "G"
    } else if (size > mbScale) {
      size / mbScale + "M"
    } else if (size > kbScale) {
      size / kbScale + "K"
    } else {
      size + ""
    }
  }

  def main(args: Array[String]): Unit = {
    execute(args)
  }
}

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
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.shuffle.{SplashOpts, SplashUtils}
import org.apache.spark.storage.ShuffleBlockId

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
          overwrite = cmd.hasOption("o")))
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
      overwrite: Boolean) {

    StorageFactoryHolder.setDefaultFactoryName(factoryName)

    private val appId = s"shuffleTest-$shuffleId"
    private val shuffleSize: Long = dataBlocks * blockSize
    private val partitionSize = shuffleSize / reducers
    private val totalShuffleSize = shuffleSize * mappers
    private val factory = StorageFactoryHolder.getFactory

    private val bytesWritten = new AtomicLong(0)
    private val bytesRead = new AtomicLong(0)
    private val totalTimeReadIndex = new AtomicLong(0)
    private var latestProgress = ""

    private def shuffleFolder = factory.getShuffleFolder(appId)

    def writeShuffle(): String = {
      if (overwrite) {
        println(s"overwrite, removing existing shuffle for $appId")
        factory.cleanShuffle(appId)
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
      if (done == 0) latestProgress = ""
      val len = latestProgress.length
      val goBack = Array.fill(len)("\b").mkString("")
      val percent = done * 100 / total
      latestProgress = f" $percent%3d%% ($done/$total)"
      print(goBack + latestProgress)
    }

    private def writeIndexFile(shuffleId: Int, mapperId: Int): Unit = {
      val blockId = ShuffleBlockId(shuffleId, mapperId, 0)
      val indexFile = factory.makeIndexFile(s"$shuffleFolder/$blockId.index")
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
      val blockId = ShuffleBlockId(shuffleId, mapperId, 0)
      val dataFile = factory.makeDataFile(s"$shuffleFolder/$blockId.data")
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
                    val toRead = is.available()
                    val buffer = new Array[Byte](readBufferSize)
                    (0 to toRead / buffer.length).foreach { _ =>
                      bytesRead.addAndGet(is.read(buffer))
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
      val bandwidth = totalShuffleSize * 1000 / duration.toMillis
      s"""|$op completed in ${duration.toMillis} milliseconds
          |    Reading index file:  ${readIndexTook.toMillis} ms
          |    storage factory:     ${factory.getClass.getCanonicalName}
          |    shuffle folder:      $shuffleFolder
          |    number of mappers:   $mappers
          |    number of reducers:  $reducers
          |    total shuffle size:  ${toSizeStr(totalShuffleSize)}
          |    bytes written:       ${toSizeStr(bytesWritten.get())}
          |    bytes read:          ${toSizeStr(bytesRead.get())}
          |    number of blocks:    $dataBlocks
          |    blocks size:         ${toSizeStr(blockSize)}
          |    partition size:      ${toSizeStr(partitionSize)}
          |    concurrent tasks:    $tasks
          |    bandwidth:           ${toSizeStr(bandwidth)}/s
          |""".stripMargin
    }

    private def getPartitionIS(blockId: ShuffleBlockId): Option[InputStream] = {
      val fileId = ShuffleBlockId(blockId.shuffleId, blockId.mapId, 0)
      val indexFile = factory.getIndexFile(s"$shuffleFolder/$fileId.index")
      val dataFile = factory.getDataFile(s"$shuffleFolder/$fileId.data")
      val start = System.nanoTime()
      try
        SplashUtils.withResources {
          val indexIs = indexFile.makeInputStream()
          indexIs.skip(blockId.reduceId * 8L)
          new DataInputStream(new BufferedInputStream(indexIs, 16))
        } { is =>

          val offset = is.readLong()
          val nextOffset = is.readLong()

          totalTimeReadIndex.addAndGet(System.nanoTime() - start)
          bytesRead.addAndGet(8 * 2)

          val dataIs = new LimitedInputStream(dataFile.makeInputStream(), nextOffset)
          dataIs.skip(offset)
          Some(new BufferedInputStream(dataIs, readBufferSize))
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

  private def toSizeStr(size: Long): String = {
    val scale = 1024L
    val kbScale: Long = scale
    val mbScale: Long = scale * kbScale
    val gbScale: Long = scale * mbScale
    val tbScale: Long = scale * gbScale
    if (size > tbScale) {
      size / tbScale + "TB"
    } else if (size > gbScale) {
      size / gbScale + "GB"
    } else if (size > mbScale) {
      size / mbScale + "MB"
    } else if (size > kbScale) {
      size / kbScale + "KB"
    } else {
      size + "B"
    }
  }

  def main(args: Array[String]): Unit = {
    execute(args)
  }
}

/*
 * Modifications copyright (C) 2018 MemVerge Inc.
 *
 * Use TmpShuffleFile interface instead of raw file.
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

import com.memverge.splash.TmpShuffleFile
import org.apache.spark.storage.TempShuffleBlockId

case class ShuffleSpillInfo(
    partitionLengths: Array[Long],
    file: TmpShuffleFile,
    blockId: TempShuffleBlockId) {
  lazy val spillSize:Long = partitionLengths.sum
}

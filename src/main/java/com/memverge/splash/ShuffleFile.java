/*
 * Copyright (C) 2018 MemVerge Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.memverge.splash;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import lombok.val;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.shuffle.PartitionLoc;

public interface ShuffleFile {

  long getSize();

  boolean delete() throws IOException;

  boolean exists() throws IOException;

  String getPath();

  InputStream makeInputStream();

  default DataInputStream makeBufferedDataInputStream() {
    return new DataInputStream(makeBufferedInputStream());
  }

  default BufferedInputStream makeBufferedInputStream() {
    return new BufferedInputStream(makeInputStream(), inputStreamBufferSize());
  }

  default BufferedInputStream makeBufferedInputStreamWithin(
      PartitionLoc loc) throws IOException {
    return makeBufferedInputStreamWithin(loc.start(), loc.end());
  }

  default BufferedInputStream makeBufferedInputStreamWithin(
      long start, long end) throws IOException {
    val inputStream = makeInputStream();
    val skipped = inputStream.skip(start);
    val defaultBufferSize = inputStreamBufferSize();

    BufferedInputStream ret;
    if (end >= skipped) {
      val limit = end - skipped;
      val bufferSize = (int) Math.min(defaultBufferSize, limit);
      ret = new BufferedInputStream(
          new LimitedInputStream(inputStream, limit),
          bufferSize);
    } else {
      ret = new BufferedInputStream(
          inputStream,
          defaultBufferSize);
    }
    return ret;
  }

  default int inputStreamBufferSize() {
    return StorageFactoryHolder.getBufferSize();
  }
}

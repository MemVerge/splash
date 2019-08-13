/*
 * Copyright (C) 2019 MemVerge Inc.
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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import lombok.val;
import org.apache.spark.unsafe.Platform;

public class SplashBufferedOutputStream extends BufferedOutputStream {

  public SplashBufferedOutputStream(OutputStream out) {
    this(out, StorageFactoryHolder.getFactory().getFileBufferSize());
  }

  public SplashBufferedOutputStream(OutputStream out, int size) {
    super(out, size);
  }

  public byte[] getBuffer() {
    return buf;
  }

  public int getBufferSize() {
    return buf.length;
  }

  public int write(byte[] bytes, long offset) throws IOException {
    val length = bytes.length - (int) offset;
    return write(bytes, Platform.BYTE_ARRAY_OFFSET + offset, length);
  }

  public int write(Object src, Long srcOffset, int length) throws IOException {
    val bufSize = getBufferSize();
    int dataRemaining = length;
    long offset = srcOffset;
    while (dataRemaining > 0) {
      val toTransfer = Math.min(bufSize, dataRemaining);
      if (count + toTransfer > bufSize && count > 0) {
        flush();
      }
      Platform.copyMemory(
          src,
          offset,
          buf,
          Platform.BYTE_ARRAY_OFFSET + count,
          toTransfer);
      count += toTransfer;
      offset += toTransfer;
      dataRemaining -= toTransfer;
    }
    return length - dataRemaining;
  }
}

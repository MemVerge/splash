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

import com.google.common.io.CountingOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.storage.TimeTrackingOutputStream;


public class ManualCloseOutputStream extends OutputStream {

  @SuppressWarnings("UnstableApiUsage")
  private CountingOutputStream stream;

  @SuppressWarnings("UnstableApiUsage")
  ManualCloseOutputStream(OutputStream out, ShuffleWriteMetrics metrics) {
    if (metrics != null) {
      out = new TimeTrackingOutputStream(metrics, out);
    }

    stream = new CountingOutputStream(out);
  }

  @Override
  public void write(int b) throws IOException {
    stream.write(b);
  }

  @Override
  @SuppressWarnings("NullableProblems")
  public void write(byte[] bytes) throws IOException {
    stream.write(bytes);
  }

  public void write(String str) throws IOException {
    stream.write(str.getBytes());
  }

  @Override
  @SuppressWarnings("NullableProblems")
  public void write(byte[] bytes, int off, int len) throws IOException {
    stream.write(bytes, off, len);
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() throws IOException {
    stream.flush();
  }

  public long getCount() {
    return stream.getCount();
  }

  public void manualClose() throws IOException {
    stream.flush();
    stream.close();
  }
}

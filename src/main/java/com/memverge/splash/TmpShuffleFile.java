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

import com.google.common.io.Closeables;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.UUID;
import lombok.val;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.shuffle.CloseAndFlushShieldOutputStream;
import org.apache.spark.shuffle.ShuffleSpillInfo;
import org.apache.spark.shuffle.SplashUtils;

public interface TmpShuffleFile extends ShuffleFile {

  TmpShuffleFile create() throws IOException;

  void swap(TmpShuffleFile other) throws IOException;

  ShuffleFile getCommitTarget();

  /**
   * Commit the tmp file to the target file. The implementation of this method
   * must be atomic. If the commit target already exists, it should be removed.
   */
  ShuffleFile commit() throws IOException;

  void recall();

  UUID uuid();

  OutputStream makeOutputStream();

  default int getBufferSize() {
    return StorageFactoryHolder.getBufferSize();
  }

  default SplashBufferedOutputStream makeBufferedOutputStream() {
    return makeBufferedOutputStream(getBufferSize());
  }

  default SplashBufferedOutputStream makeBufferedOutputStream(int bufferSize) {
    return new SplashBufferedOutputStream(makeOutputStream(), bufferSize);
  }

  default DataOutputStream makeBufferedDataOutputStream() {
    return new DataOutputStream(makeBufferedOutputStream());
  }

  default ManualCloseOutputStream makeManualCloseOutputStream(
      ShuffleWriteMetrics metrics) {
    return new ManualCloseOutputStream(makeOutputStream(), metrics);
  }

  default ManualCloseOutputStream makeBufferedManualCloseOutputStream(
      ShuffleWriteMetrics metrics) {
    return new ManualCloseOutputStream(makeBufferedOutputStream(), metrics);
  }

  default ManualCloseOutputStream makeBufferedManualCloseOutputStream() {
    return makeBufferedManualCloseOutputStream(null);
  }

  default boolean supportFastMerge() {
    return false;
  }

  default long[] fastMerge(ShuffleSpillInfo[] spills) throws IOException {
    throw new NotImplementedException("do not support fast merge");
  }
}

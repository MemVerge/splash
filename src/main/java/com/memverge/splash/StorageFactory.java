/*
 * Copyright (C) 2018 MemVerge Corp
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

import java.io.IOException;
import java.util.Collection;
import org.apache.spark.SparkConf;

public interface StorageFactory {

  // for TmpShuffleFile
  TmpShuffleFile makeSpillFile() throws IOException;

  TmpShuffleFile makeDataFile(String path) throws IOException;

  TmpShuffleFile makeIndexFile(String path) throws IOException;

  // for getting file reference
  ShuffleFile getDataFile(String path) throws IOException;

  ShuffleFile getIndexFile(String path) throws IOException;

  // retrieve listeners
  Collection<ShuffleListener> getListeners();

  // metadata
  default void setConf(SparkConf conf) {
  }

  String getShuffleFolder(String appId);

  default void cleanShuffle(String appId) throws IOException {
    getDataFile(getShuffleFolder(appId)).delete();
  }

  int getShuffleFileCount(String appId);

  int getTmpFileCount() throws IOException;

  // cleanup
  void reset();
}

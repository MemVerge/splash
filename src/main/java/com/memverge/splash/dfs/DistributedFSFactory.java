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
package com.memverge.splash.dfs;

import com.memverge.splash.*;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class DistributedFSFactory implements StorageFactory {
  private final DistributedFSTempFolder tempFolder = DistributedFSTempFolder.getInstance();

  @Override
  public TmpShuffleFile makeSpillFile() {
    return DistributedFSTmpShuffleFile.make();
  }

  @Override
  public TmpShuffleFile makeDataFile(String path) throws IOException {
    return DistributedFSTmpShuffleFile.make(getDataFile(path));
  }

  @Override
  public TmpShuffleFile makeIndexFile(String path) throws IOException {
    return DistributedFSTmpShuffleFile.make(new DistributedFSShuffleFile(path));
  }

  @Override
  public ShuffleFile getDataFile(String path) {
    return new DistributedFSShuffleFile(path);
  }

  @Override
  public ShuffleFile getIndexFile(String path) {
    return new DistributedFSShuffleFile(path);
  }

  @Override
  public Collection<ShuffleListener> getListeners() {
    return Collections.emptyList();
  }

  @Override
  public String getShuffleFolder(String appId) {
    return tempFolder.getShufflePath(appId);
  }

  @Override
  public int getShuffleFileCount(String appId) {
    try {
      return tempFolder.countShuffleFile(appId);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public int getTmpFileCount() throws IOException {
    return tempFolder.countTmpFile();
  }

  @Override
  public void reset() {
    tempFolder.reset();
  }
}

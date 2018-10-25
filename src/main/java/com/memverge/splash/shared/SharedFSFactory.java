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
package com.memverge.splash.shared;

import com.memverge.splash.ShuffleFile;
import com.memverge.splash.ShuffleListener;
import com.memverge.splash.StorageFactory;
import com.memverge.splash.TmpShuffleFile;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class SharedFSFactory implements StorageFactory {

  private SharedFSFolder folder = SharedFSFolder.getInstance();

  @Override
  public TmpShuffleFile makeSpillFile() throws IOException {
    return SharedFSTmpShuffleFile.make();
  }

  @Override
  public TmpShuffleFile makeDataFile(String path) throws IOException {
    return SharedFSTmpShuffleFile.make(getDataFile(path));
  }

  @Override
  public TmpShuffleFile makeIndexFile(String path) throws IOException {
    return SharedFSTmpShuffleFile.make(new SharedFSShuffleFile(path));
  }

  @Override
  public ShuffleFile getDataFile(String path) {
    return new SharedFSShuffleFile(path);
  }

  @Override
  public ShuffleFile getIndexFile(String path) {
    return new SharedFSShuffleFile(path);
  }

  @Override
  public Collection<ShuffleListener> getListeners() {
    // No listeners for this implementation
    return Collections.emptyList();
  }

  @Override
  public String getShuffleFolder(String appId) {
    return folder.getShufflePath(appId);
  }

  @Override
  public int getShuffleFileCount(String appId) {
    return folder.countShuffleFile(appId);
  }

  @Override
  public int getTmpFileCount() {
    return folder.countTmpFile();
  }

  @Override
  public void reset() {
    folder.reset();
  }
}

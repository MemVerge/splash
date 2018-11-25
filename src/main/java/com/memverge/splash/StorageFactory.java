/*
 * Copyright (c) 2018 MemVerge Inc.
 */
package com.memverge.splash;

import java.io.IOException;
import java.util.Collection;

public interface StorageFactory {

  static int tmp_file_count = 0;
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
  String getShuffleFolder(String appId);

  int getShuffleFileCount(String appId);

  int getTmpFileCount() throws IOException;

  // cleanup
  void reset();
}

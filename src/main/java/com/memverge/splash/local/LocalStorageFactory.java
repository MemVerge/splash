/*
 * Copyright (c) 2018 MemVerge Inc.
 */
package com.memverge.splash.local;

import com.memverge.splash.ShuffleFile;
import com.memverge.splash.ShuffleListener;
import com.memverge.splash.StorageFactory;
import com.memverge.splash.TmpShuffleFile;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;

public class LocalStorageFactory implements StorageFactory {

  @Override
  public TmpShuffleFile makeSpillFile() throws IOException {
    //TODO:
    return null;
  }

  @Override
  public TmpShuffleFile makeDataFile(String path) throws IOException {
    //TODO:
    return null;
  }

  @Override
  public TmpShuffleFile makeIndexFile(String path) throws IOException {
    //TODO:
    return null;
  }

  @Override
  public ShuffleFile getDataFile(String path) throws IOException {
    //TODO:
    return null;
  }

  @Override
  public ShuffleFile getIndexFile(String path) throws IOException {
    //TODO:
    return null;
  }

  @Override
  public Collection<ShuffleListener> getListeners() {
    // No listeners for this implementation
    return Collections.emptyList();
  }

  @Override
  public String getShuffleFolder(String appId) {
    //TODO: Update this if it doesn't suit your needs
    return Paths.get(getTmpPath(appId), "shuffle").toString();
  }

  @Override
  public int getShuffleFileCount(String appId) {
    //TODO:
    return 0;
  }

  @Override
  public int getTmpFileCount() {
    //TODO:
    return 0;
  }

  @Override
  public void reset() {
    //TODO:
  }

  private String getTmpPath(String folder) {
    //TODO: Update this if it doesn't suit your needs
    String tmpFolder = System.getProperty("java.io.tmpdir");
    Path path = Paths.get(tmpFolder, getUser());
    return Paths.get(path.toString(), folder).toString();
  }

  private String getUser() {
    //TODO: Update this if it doesn't suit your needs
    String user = System.getProperty("user.name");
    return StringUtils.isEmpty(user) ? "unknown" : user;
  }
}

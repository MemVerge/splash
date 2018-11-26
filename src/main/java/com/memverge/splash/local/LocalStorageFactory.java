/*
 * Copyright (c) 2018 MemVerge Inc.
 */
package com.memverge.splash.local;

import com.memverge.splash.ShuffleFile;
import com.memverge.splash.ShuffleListener;
import com.memverge.splash.StorageFactory;
import com.memverge.splash.TmpShuffleFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalStorageFactory implements StorageFactory {
  private static final Logger log = LoggerFactory
      .getLogger(LocalShuffleFile.class);

  @Override
  public TmpShuffleFile makeSpillFile() throws IOException {
    return LocalTmpShuffleFile.make();
  }

  @Override
  public TmpShuffleFile makeDataFile(String path) throws IOException {
    return LocalTmpShuffleFile.make(getDataFile(path));
  }

  @Override
  public TmpShuffleFile makeIndexFile(String path) throws IOException {
    return LocalTmpShuffleFile.make(new LocalShuffleFile(path));
  }

  @Override
  public ShuffleFile getDataFile(String path) throws IOException {
    return new LocalShuffleFile(path);
  }

  @Override
  public ShuffleFile getIndexFile(String path) throws IOException {
    return new LocalShuffleFile(path);
  }

  @Override
  public Collection<ShuffleListener> getListeners() {
    // No listeners for this implementation
    return Collections.emptyList();
  }

  @Override
  public String getShuffleFolder(String appId) {
    return Paths.get(getTmpPath(appId), "shuffle").toString();
  }

  @Override
  public int getShuffleFileCount(String appId) {
    File file = Paths.get(getTmpPath(appId)).toFile();
    List<String> ret = new ArrayList<>();
    new LocalShuffleFile().listAll(file,ret);
    return ret.size();
  }

  @Override
  public int getTmpFileCount() throws IOException{
    return new LocalTmpShuffleFile().getTmpFileCount();
  }

  @Override
  public void reset() {
    LocalShuffleFile.deleteAll();
  }

  private String getTmpPath(String folder) {
    String tmpFolder = System.getProperty("java.io.tmpdir");
    Path path = Paths.get(tmpFolder, getUser());
    return Paths.get(path.toString(), folder).toString();
  }

  private String getUser() {
    String user = System.getProperty("user.name");
    return StringUtils.isEmpty(user) ? "unknown" : user;
  }
}

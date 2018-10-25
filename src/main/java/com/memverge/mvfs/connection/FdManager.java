/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.connection;

import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FdManager {
  private static class FdManagerHolder {

    private static final FdManager INSTANCE = new FdManager();
  }

  public static FdManager getInstance() {
    return FdManager.FdManagerHolder.INSTANCE;
  }

  private Map<Integer, RandomAccessFile> fdFileMap = new ConcurrentHashMap<>();
  private Map<Integer, String> fdPathMap = new ConcurrentHashMap<>();

  boolean has(int fd) {
    return fdFileMap.containsKey(fd);
  }

  void put(int fd, String path, RandomAccessFile file) {
    fdFileMap.put(fd, file);
    fdPathMap.put(fd, path);
  }

  RandomAccessFile getFile(int fd) {
    return fdFileMap.get(fd);
  }

  String getPath(int fd) {
    return fdPathMap.get(fd);
  }

  void remove(int fd) {
    fdFileMap.remove(fd);
    fdPathMap.remove(fd);
  }

  public int openFdSize() {
    return fdFileMap.size();
  }
}

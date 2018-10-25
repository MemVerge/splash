/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.dmo;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.memverge.mvfs.connection.MVFSConnectOptions;
import com.memverge.mvfs.connection.MVFSConnectionMgr;
import com.memverge.mvfs.error.MVFSException;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@SuppressWarnings("UnstableApiUsage")
public class FileHandleCache {

  private ConcurrentMap<String, LoadingCache<String, CacheItem>> cacheMap;

  private static class FileHandleCacheHolder {

    private static final FileHandleCache INSTANCE = new FileHandleCache();
  }

  public static FileHandleCache getInstance() {
    return FileHandleCacheHolder.INSTANCE;
  }

  private int cacheSize = 2000;

  private FileHandleCache() {
    cacheMap = new ConcurrentHashMap<>();
    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> {
          log.info("clean up file handle cache before exiting.");
          reset();

          log.info("disconnect all dmo sockets.");
          MVFSConnectionMgr.cleanup();
        }));
  }

  public void reset() {
    cacheMap.values().forEach(Cache::invalidateAll);
    cacheMap.clear();
  }

  int getReadOnlyFd(DMOFile file) throws IOException {
    val cache = getCache(file.getSocketPath());
    int fd;
    try {
      fd = cache.get(file.getDmoId()).getFd();
    } catch (ExecutionException ex) {
      val msg = String.format("failed to retrieve read only fd for %s from "
          + "cache.  error: %s", file.getDmoId(), ex.getMessage());
      log.warn(msg);
      throw new MVFSException(msg, file);
    }
    return fd;
  }

  long getSize(DMOFile file) throws IOException {
    val cache = getCache(file.getSocketPath());
    long size;
    try {
      size = cache.get(file.getDmoId()).getSize();
    } catch (ExecutionException ex) {
      val msg = String.format("failed to retrieve read only file size for %s "
          + "from cache.  error: %s", file.getDmoId(), ex.getMessage());
      log.warn(msg);
      throw new MVFSException(msg, file);
    }
    return size;
  }

  void invalidate(DMOFile file) {
    getCache(file.getSocketPath()).invalidate(file.getDmoId());
  }

  public void invalidate(String socket) {
    val cache = getCache(socket);
    cache.invalidateAll();
    cache.cleanUp();
  }

  public void invalidate() {
    invalidate(MVFSConnectOptions.defaultSocket());
  }

  public long size(String socket) {
    return getCache(socket).size();
  }

  public long size() {
    return size(MVFSConnectOptions.defaultSocket());
  }

  public void setCacheSize(int newSize) {
    if (newSize != cacheSize) {
      val cores = Runtime.getRuntime().availableProcessors();
      if (newSize < cores) {
        log.warn("cache size should be at least the number of cores."
            + "  reset it to the number of cores: {}", cores);
        newSize = cores;
      }
      cacheSize = newSize;
      reset();
    }
  }

  private LoadingCache<String, CacheItem> getCache(String socket) {
    return cacheMap.computeIfAbsent(socket, this::fdCache);
  }

  private LoadingCache<String, CacheItem> fdCache(String socket) {
    log.info("Create fd cache for dmo socket {}", socket);
    return CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .expireAfterAccess(1, TimeUnit.MINUTES)
        .removalListener((RemovalListener<String, CacheItem>) entry -> {
          try {
            new DMOFile(socket, entry.getKey()).close(entry.getValue().getFd());
          } catch (IOException e) {
            log.warn("error closing file {}.  error: {}",
                entry.getKey(), e.getMessage());
          }
        })
        .build(new CacheLoader<String, CacheItem>() {
          @Override
          public CacheItem load(String dmoId) throws IOException {
            val dmoFile = new DMOFile(socket, dmoId);
            val attr = dmoFile.open(false);
            return CacheItem.from(attr);
          }
        });
  }

}


@Value
@AllArgsConstructor
class CacheItem {

  static CacheItem from(ObjectAttr attr) {
    return new CacheItem(attr.getRc(), attr.getSize(), attr.isFile());
  }

  private Integer fd;
  private long size;
  private boolean isFile;
}

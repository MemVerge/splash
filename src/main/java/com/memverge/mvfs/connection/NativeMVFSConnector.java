/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package com.memverge.mvfs.connection;

import com.memverge.mvfs.dmo.ObjectAttr;
import com.memverge.mvfs.dmo.PrefetchItem;
import com.memverge.mvfs.utils.NativeLoader;
import java.nio.ByteBuffer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NativeMVFSConnector extends MVFSConnector {

  private static boolean jniLoaded = false;

  NativeMVFSConnector(MVFSConnectOptions options) {
    super(options);
    if (!jniLoaded) {
      new NativeLoader("mvfs_jni").load();
      jniLoaded = true;
    }
  }

  @Override
  protected native int connect(MVFSConnectOptions options);

  @Override
  protected native int disconnect();

  @Override
  public native int broadcast(String path);

  @Override
  public native int prefetch(PrefetchItem prefetchItem);

  @Override
  public native int create(String path, int chunkSize, int numOfReplicas);

  @Override
  public native int unlink(String path);

  @Override
  public native int open(String path, int flags);

  @Override
  public native ObjectAttr openWithAttr(String path, int flags);

  @Override
  public native int close(int fd);

  @Override
  protected native int _exists(String path);

  @Override
  public native int mkdir(String path);

  @Override
  public native int rmdir(String path);

  @Override
  public native int rename(String srcPath, String dstPath);

  @Override
  public native int read(int fd, byte[] buf, int size, long offset,
      long bufOffset);

  @Override
  public native int write(int fd, byte[] buf, int size, long offset,
      long bufOffset);

  @Override
  public native ByteBuffer mmap(String path, long capacity);

  @Override
  public native ObjectAttr getAttr(String path);

  @Override
  public native int setAttr(String path, ObjectAttr attr);

  @Override
  public native ObjectAttr getAttr(int fd);

  @Override
  public native int setAttr(int fd, ObjectAttr attr);

  @Override
  public native String[] listAll();

  @Override
  public native String[] list(String path);
}

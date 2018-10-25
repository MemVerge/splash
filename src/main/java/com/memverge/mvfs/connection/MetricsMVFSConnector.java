/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.connection;

import com.memverge.mvfs.dmo.ObjectAttr;
import com.memverge.mvfs.dmo.PrefetchItem;
import com.memverge.mvfs.metrics.Metric;
import java.io.IOException;
import java.nio.ByteBuffer;
import lombok.val;

public class MetricsMVFSConnector extends MVFSConnector {

  private MVFSConnector innerConnector;

  MetricsMVFSConnector(MVFSConnector connector) {
    super(connector.getOptions());
    innerConnector = connector;
  }

  @Override
  public int create(String path, int chunkSize, int numOfReplicas)
      throws IOException {
    val start = System.nanoTime();
    val rc = innerConnector.create(path, chunkSize, numOfReplicas);
    Metric.Create.add(System.nanoTime() - start, rc);
    return rc;
  }

  @Override
  public int unlink(String path) {
    val start = System.nanoTime();
    val rc = innerConnector.unlink(path);
    Metric.Delete.add(System.nanoTime() - start, rc);
    return rc;
  }

  @Override
  public int open(String path, int flags) {
    val start = System.nanoTime();
    val rc = innerConnector.open(path, flags);
    val bytes = rc > 0 ? 0 : rc;
    Metric.Open.add(System.nanoTime() - start, bytes);
    return rc;
  }

  @Override
  public ObjectAttr openWithAttr(String path, int flags) {
    val start = System.nanoTime();
    val attr = innerConnector.openWithAttr(path, flags);
    val bytes = attr.getRc() > 0 ? 0 : attr.getRc();
    Metric.Open.add(System.nanoTime() - start, bytes);
    return attr;
  }

  @Override
  public int close(int fd) {
    val start = System.nanoTime();
    val rc = innerConnector.close(fd);
    Metric.Close.add(System.nanoTime() - start, rc);
    return rc;
  }

  @Override
  protected int _exists(String path) {
    val start = System.nanoTime();
    val rc = innerConnector._exists(path);
    Metric.Exists.add(System.nanoTime() - start, rc);
    return rc;
  }

  @Override
  public int mkdir(String path) {
    val start = System.nanoTime();
    val rc = innerConnector.mkdir(path);
    Metric.Mkdir.add(System.nanoTime() - start, rc);
    return rc;
  }

  @Override
  public int rmdir(String path) {
    val start = System.nanoTime();
    val rc = innerConnector.rmdir(path);
    Metric.Rmdir.add(System.nanoTime() - start, rc);
    return rc;
  }

  @Override
  public int rename(String srcPath, String dstPath) {
    val start = System.nanoTime();
    val rc = innerConnector.rename(srcPath, dstPath);
    Metric.Rename.add(System.nanoTime() - start, rc);
    return rc;
  }

  @Override
  public int read(int fd, byte[] buf, int size, long offset, long bufOffset) {
    val start = System.nanoTime();
    val rc = innerConnector.read(fd, buf, size, offset, bufOffset);
    Metric.Read.add(System.nanoTime() - start, rc);
    return rc;
  }

  @Override
  public int write(int fd, byte[] buf, int size, long offset, long bufOffset) {
    val start = System.nanoTime();
    val rc = innerConnector.write(fd, buf, size, offset, bufOffset);
    Metric.Write.add(System.nanoTime() - start, rc);
    return rc;
  }

  @Override
  public ByteBuffer mmap(String path, long capacity) throws IOException {
    val start = System.nanoTime();
    ByteBuffer ret;
    try {
      ret = innerConnector.mmap(path, capacity);
    } finally {
      Metric.Mmap.add(System.nanoTime() - start, 0);
    }
    return ret;
  }

  @Override
  public ObjectAttr getAttr(String path) {
    val start = System.nanoTime();
    val attr = innerConnector.getAttr(path);
    Metric.GetAttr.add(System.nanoTime() - start, attr.getRc());
    return attr;
  }

  @Override
  public int setAttr(String path, ObjectAttr attr) {
    val start = System.nanoTime();
    val rc = innerConnector.setAttr(path, attr);
    Metric.SetAttr.add(System.nanoTime() - start, rc);
    return rc;
  }

  @Override
  public ObjectAttr getAttr(int fd) {
    val start = System.nanoTime();
    val attr = innerConnector.getAttr(fd);
    Metric.GetAttr.add(System.nanoTime() - start, attr.getRc());
    return attr;
  }

  @Override
  public int setAttr(int fd, ObjectAttr attr) {
    val start = System.nanoTime();
    val rc = innerConnector.setAttr(fd, attr);
    Metric.SetAttr.add(System.nanoTime() - start, rc);
    return rc;
  }

  @Override
  public String[] listAll() {
    val start = System.nanoTime();
    val ret = innerConnector.listAll();
    Metric.List.add(System.nanoTime() - start, 0);
    return ret;
  }

  @Override
  public String[] list(String path) {
    val start = System.nanoTime();
    val ret = innerConnector.list(path);
    Metric.List.add(System.nanoTime() - start, 0);
    return ret;
  }

  @Override
  protected int connect(MVFSConnectOptions options) {
    val start = System.nanoTime();
    val rc = innerConnector.connect(options);
    Metric.Connect.add(System.nanoTime() - start, 0);
    return rc;
  }

  @Override
  protected int disconnect() {
    val start = System.nanoTime();
    val rc = innerConnector.disconnect();
    Metric.Disconnect.add(System.nanoTime() - start, 0);
    return rc;
  }

  @Override
  public int broadcast(String path) {
    val start = System.nanoTime();
    val rc = innerConnector.broadcast(path);
    Metric.Broadcast.add(System.nanoTime() - start, 0);
    return rc;
  }

  @Override
  public int prefetch(PrefetchItem prefetchItem) {
    val start = System.nanoTime();
    val rc = innerConnector.prefetch(prefetchItem);
    Metric.Prefetch.add(System.nanoTime() - start, rc);
    return rc;
  }
}

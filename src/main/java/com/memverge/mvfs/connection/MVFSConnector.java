/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package com.memverge.mvfs.connection;

import com.memverge.mvfs.dmo.DMOFile;
import com.memverge.mvfs.dmo.ObjectAttr;
import com.memverge.mvfs.dmo.PrefetchItem;
import com.memverge.mvfs.error.MVFSConnectFailed;
import com.memverge.mvfs.error.MVFSException;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public abstract class MVFSConnector implements Closeable {

  @Getter
  private MVFSConnectOptions options;
  @Getter
  private boolean connected = false;

  private final Object closeLock;

  public MVFSConnector(MVFSConnectOptions options) {
    this.closeLock = new Object();
    if (options == null) {
      this.options = new MVFSConnectOptions();
    } else {
      this.options = options;
    }
  }

  public abstract int create(String path, int chunkSize, int numOfReplicas)
      throws IOException;

  public abstract int unlink(String path);

  public abstract int open(String path, int flags);

  public abstract ObjectAttr openWithAttr(String path, int flags);

  public abstract int close(int fd);

  protected abstract int _exists(String path);

  public abstract int mkdir(String path);

  public abstract int rmdir(String path);

  public abstract int rename(String srcPath, String dstPath);

  public abstract int read(int fd, byte[] buf, int size, long offset,
      long bufOffset);

  public abstract int write(int fd, byte[] buf, int size, long offset,
      long bufOffset);

  public abstract ByteBuffer mmap(String path, long capacity)
      throws IOException;

  public abstract ObjectAttr getAttr(String path);

  public abstract int setAttr(String path, ObjectAttr attr);

  public abstract ObjectAttr getAttr(int fd);

  public abstract int setAttr(int fd, ObjectAttr attr);

  public abstract String[] listAll();

  public abstract String[] list(String path);

  protected abstract int connect(MVFSConnectOptions options);

  protected abstract int disconnect();

  public abstract int broadcast(String path);

  public abstract int prefetch(PrefetchItem prefetchItem);

  public boolean exists(String path) throws IOException {
    val rc = _exists(path);
    if (rc != 0 && rc != DMOFile.ERROR_FILE_NOT_FOUND) {
      String msg = String.format("failed to check existence of '%s'", path);
      throw new IOException(msg);
    }
    return rc == 0;
  }

  public final int connect() throws MVFSConnectFailed {
    int rc = 0;
    synchronized (closeLock) {
      if (connected) {
        return rc;
      }
      val connOptions = getOptions();
      val socket = getSocket();
      val warmUp = getOptions().isWarmUp();
      log.info("Connecting to DMO at socket {}, warmUp {}", socket, warmUp);
      rc = connect(connOptions);
      if (rc < 0) {
        log.error("Connect to DMO at {} failed: {}", socket, rc);
        connected = false;
        throw new MVFSConnectFailed(socket, rc);
      }
      log.info("Successfully connected to DMO at {}", socket);
      connected = true;
    }
    return rc;
  }

  @Override
  public final void close() throws IOException {
    synchronized (closeLock) {
      if (!connected) {
        return;
      }
      connected = false;
    }
    val rc = disconnect();
    if (rc < 0) {
      connected = true;
      val msg = String.format("failed to disconnect dmo at socket %s, rc: %d",
          getSocket(), rc);
      log.error(msg);
      throw new MVFSException(msg);
    } else {
      log.info("successfully disconnected dmo at socket {} .", getSocket());
    }
  }

  private String getSocket() {
    return getOptions().getSocket();
  }
}

/*
 * Copyright (c) 2018 MemVerge Inc.
 */
package com.memverge.mvfs.dmo;

import com.memverge.mvfs.connection.MVFSConnectOptions;
import com.memverge.mvfs.connection.MVFSConnectionMgr;
import com.memverge.mvfs.connection.MVFSConnector;
import com.memverge.mvfs.error.MVFSDirNotEmpty;
import com.memverge.mvfs.error.MVFSException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class DMOFile {

  static final int CHUNK_SIZE = initDefaultChunkSize();
  private static final int REPLICAS = 0;

  public static final int ERROR_FILE_NOT_FOUND = -1003;
  public static final int ERROR_FILE_EXISTS = -1008;
  public static final int ERROR_INVALID_ARG = -1009;
  public static final int ERROR_DIR_NOT_EMPTY = -1015;
  public static final int ERROR_STALE_HANDLE = -1016;

  private MVFSConnector mvfsConn;

  private static FileHandleCache fdCache = FileHandleCache.getInstance();

  String dmoId;

  @Getter
  private final int chunkSize;

  @Getter
  private final String socketPath;

  private long size = -1;

  public DMOFile(String dmoId) throws IOException {
    this(null, dmoId);
  }

  public DMOFile(String socketPath, String dmoId) throws IOException {
    this(socketPath, dmoId, CHUNK_SIZE);
  }

  public DMOFile(String socketPath, String dmoId, int chunkSize)
      throws IOException {
    if (StringUtils.isBlank(socketPath)) {
      socketPath = MVFSConnectOptions.defaultSocket();
    }

    if (chunkSize <= 0) {
      chunkSize = CHUNK_SIZE;
    }
    mvfsConn = MVFSConnectionMgr.get(socketPath);
    this.dmoId = formalizeId(dmoId);
    this.socketPath = socketPath;
    this.chunkSize = chunkSize;
  }

  private static int initDefaultChunkSize() {
    val defaultChunkSize = 1024 * 1024;
    int ret = defaultChunkSize;

    val chunkSizeStr = System.getenv("JMVFS_CHUNK_SIZE");
    if (StringUtils.isNotEmpty(chunkSizeStr)) {
      try {
        ret = Integer.parseInt(chunkSizeStr);
        if (ret < 4 * 1024) {
          throw new IllegalArgumentException("Smallest chunk size 4k");
        }
      } catch (IllegalArgumentException ex) {
        log.error("{} is not a valid chunk size, user default {}.",
            chunkSizeStr, defaultChunkSize, ex);
      }
    }
    return ret;
  }

  MVFSConnector getMvfsConn() throws IOException {
    if (mvfsConn == null) {
      mvfsConn = MVFSConnectionMgr.get(socketPath);
    }
    return mvfsConn;
  }

  public DMOFile create() throws IOException {
    log.debug("mvfs_create() starting for '{}'", dmoId);
    int rc;
    if (dmoId.endsWith("/")) {
      rc = getMvfsConn().mkdir(dmoId);
    } else {
      rc = getMvfsConn().create(dmoId, chunkSize, REPLICAS);
    }
    if (rc < 0) {
      throw new MVFSException("mvfs_create() failed.", dmoId, rc);
    } else {
      log.debug("dmo {} created on mvfs, chunk size {}.", dmoId, chunkSize);
    }
    return this;
  }

  public ObjectAttr attribute(int fd) throws IOException {
    val ret = getMvfsConn().getAttr(fd);
    return checkGetAttrResult(ret);
  }

  private ObjectAttr checkGetAttrResult(ObjectAttr ret)
      throws FileNotFoundException, MVFSException {
    int rc;
    if (ret == null) {
      // this should NEVER happen
      log.error("initialize attributes for '{}' failed.", dmoId);
      rc = -1;
    } else {
      rc = ret.getRc();
    }
    if (rc == ERROR_FILE_NOT_FOUND) {
      throw new FileNotFoundException("Cannot find DMO " + dmoId);
    } else if (rc < 0) {
      throw new MVFSException("get attributes failed.", dmoId, rc);
    }
    return ret;
  }

  public ByteBuffer mmap(long capacity) throws IOException {
    return getMvfsConn().mmap(getDmoId(), capacity);
  }

  public ObjectAttr attribute() throws IOException {
    val ret = getMvfsConn().getAttr(dmoId);
    return checkGetAttrResult(ret);
  }

  ObjectAttr open(boolean create) throws IOException {
    ObjectAttr attr = getMvfsConn().openWithAttr(dmoId, 0);
    int fd = attr.getRc();
    if (fd < 0) {
      if (fd == ERROR_FILE_NOT_FOUND && create) {
        log.debug("'{}' not found, try to create.", dmoId);
        create();
        attr = open(false);
      } else if (fd == ERROR_FILE_NOT_FOUND) {
        throw new FileNotFoundException(
            String.format("Cannot find DMO '%s'.", dmoId));
      } else {
        throw new MVFSException("mvfs_open failed.", dmoId, fd);
      }
    } else {
      log.debug("dmo opened '{}', fd {}.", dmoId, fd);
    }
    return attr;
  }

  void close(int fd) throws IOException {
    val rc = getMvfsConn().close(fd);
    if (rc < 0) {
      throw new MVFSException("close file failed.", dmoId, rc);
    }
    log.debug("dmo closed '{}', rc {}.", dmoId, rc);
    size = -1;
  }

  private String formalizeId(String id) throws MVFSException {
    if (StringUtils.isEmpty(id)) {
      throw new MVFSException("empty dmo id is not allowed.");
    } else if (!id.startsWith("/")) {
      id = "/" + id;
    }
    return id;
  }

  public void append(File file) throws IOException {
    write(file, true);
  }

  public void write(File file) throws IOException {
    write(file, false);
  }

  private void write(File file, boolean append) throws IOException {
    try (val fileIs = new FileInputStream(file)) {
      write(fileIs, append);
    }
  }

  public void write(InputStream input, boolean append) throws IOException {
    try (val dmoOs = new DMOOutputStream(this, append)) {
      IOUtils.copy(input, dmoOs);
    }
  }

  public long getSize() {
    if (size == -1) {
      try {
        log.debug("get attribute with path for {}", dmoId);
        size = attribute().getSize();
      } catch (IOException e) {
        log.error("failed to get size of '{}'.", dmoId, e);
      }
    }
    return size;
  }

  public long getSize(int fd) {
    if (size == -1) {
      try {
        log.debug("get attribute with fd for {}", dmoId);
        size = attribute(fd).getSize();
      } catch (IOException e) {
        log.error("failed to get size of '{}'.", dmoId, e);
      }
    }
    return size;
  }

  public byte[] read() throws IOException {
    final byte[] ret;
    try (val dmoIs = new DMOInputStream(this)) {
      val size = (int) getSize();
      ret = new byte[size];
      val bytesRead = dmoIs.read(ret);
      if (bytesRead < size) {
        log.warn("actual bytes read {} of {} is smaller than size {}.",
            bytesRead, dmoId, size);
      }
    }
    return ret;
  }

  void rmdir() throws IOException {
    val rc = getMvfsConn().rmdir(dmoId);
    val success = !(rc < 0 && rc != ERROR_FILE_NOT_FOUND);
    if (!success) {
      throw new MVFSException("dmo rmdir failed.", dmoId, rc);
    } else {
      log.debug("rmdir success for '{}'.", dmoId);
    }
  }

  void unlink() throws IOException {
    fdCache.invalidate(this);
    val rc = getMvfsConn().unlink(dmoId);
    val success = !(rc < 0 && rc != ERROR_FILE_NOT_FOUND);
    if (!success) {
      if (rc == ERROR_DIR_NOT_EMPTY) {
        throw new MVFSDirNotEmpty(dmoId);
      } else {
        throw new MVFSException("dmo unlink failed.", dmoId, rc);
      }
    } else {
      log.debug("unlink success for '{}'.", dmoId);
    }
  }

  public void delete() throws IOException {
    try {
      unlink();
    } catch (MVFSDirNotEmpty ex) {
      rmdir();
    }
  }

  public void forceDelete() {
    try {
      if (exists()) {
        delete();
      }
    } catch (IOException ex) {
      log.warn("Error while deleting '{}'.  Ignore it due to force.", dmoId);
    }
  }

  public void rename(String tgtId) throws IOException {
    fdCache.invalidate(this);
    val rc = getMvfsConn().rename(dmoId, tgtId);
    if (rc == 0) {
      log.debug("rename {} to {}.", dmoId, tgtId);
      dmoId = formalizeId(tgtId);
    } else if (rc == ERROR_FILE_NOT_FOUND) {
      throw new FileNotFoundException("Cannot find source DMO " + dmoId);
    } else if (rc == ERROR_FILE_EXISTS) {
      val msg = String.format("Target DMO %s already exists.", tgtId);
      throw new FileAlreadyExistsException(msg);
    } else {
      throw new MVFSException("Rename DMO to " + tgtId + " failed.", dmoId, rc);
    }
  }

  public boolean exists() throws IOException {
    return getMvfsConn().exists(dmoId);
  }

  public String[] list() throws IOException {
    log.debug("list contents for '{}'", dmoId);
    val ret = getMvfsConn().list(dmoId);
    return ret == null ? new String[0] : ret;
  }

  public boolean isFolder() throws IOException {
    return !attribute().isFile();
  }

  public String getDmoId() {
    return dmoId;
  }

  public static String[] listAll(String socket) throws IOException {
    log.debug("list all for socket {}", socket);
    return new DMOFile(socket, "/").getMvfsConn().listAll();
  }

  public static String[] listAll() throws IOException {
    return listAll(null);
  }

  public static void reset(String socket) throws IOException {
    try {
      fdCache.reset();
      new DMOFile(socket, "/").delete();
    } catch (FileNotFoundException ex) {
      log.debug("files for '{}' already cleared.", socket);
    }
  }

  public static void reset() throws IOException {
    reset(null);
  }

  public static void safeReset() {
    try {
      reset(null);
    } catch (IOException e) {
      log.warn("Reset failed.", e);
    }
  }

  public String getPath(String child) {
    return StringUtils.stripEnd(getDmoId(), "/") + "/"
        + StringUtils.stripStart(child, "/");
  }

  public void broadcast() throws IOException {
    val rc = getMvfsConn().broadcast(dmoId);
    if (rc < 0) {
      throw new MVFSException("broadcast file failed.", this, rc);
    }
  }

  void prefetch(long offset, long size) throws IOException {
    if (size != 0) {
      val prefetchItem = new PrefetchItem(dmoId, offset, size);
      val rc = getMvfsConn().prefetch(prefetchItem);
      if (rc < 0) {
        val msg = String.format("prefetch %s failed.", prefetchItem.toString());
        throw new MVFSException(msg, this, rc);
      }
    } else {
      log.info("ignore prefetch for {} since size is 0.", dmoId);
    }
  }
}

/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package com.memverge.mvfs.hdfs;

import com.memverge.mvfs.connection.MVFSConnectOptions;
import com.memverge.mvfs.dmo.DMOFile;
import com.memverge.mvfs.dmo.DMOOutputStream;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

@Slf4j
public class DMOFileSystem extends FileSystem {

  private static final String SCHEMA = "dmo";
  private static final String SCHEMA_HEAD = SCHEMA + "://";

  /**
   * <p>
   * Block size in HDFS only means this:
   *
   * <br /> 1) when storing file, HDFS will split file in blocks and store them on different
   * machines to evenly distribute data over multiple machines
   *
   * <br /> 2) when reading one mapper task will be run per block allowing parallelization when
   * processing large files
   * </p>
   * To avoid small tasks, set minimal block size to 128M.
   */
  private static final long MIN_BLOCK_SIZE = 128 * 1024 * 1024;
  private static final int BUFFER_SIZE = 8 * 1024;

  private Path workingDir = null;
  private URI uri;
  private String socket;
  private Configuration conf = null;

  @Override
  public String getScheme() {
    return SCHEMA;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    val dmoFile = getDmoFile(path);
    val hdfsDmoIs = new HdfsDmoInputStream(dmoFile, statistics, bufferSize);
    return new FSDataInputStream(hdfsDmoIs);
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    if (conf == null) {
      throw new IllegalArgumentException("conf should not be null.");
    }
    socket = conf.get("mvfs.socket", MVFSConnectOptions.defaultSocket());
    if (uri == null) {
      val uriStr = SCHEMA_HEAD + socket + "/";
      try {
        uri = new URI(uriStr);
      } catch (URISyntaxException e) {
        log.error("{} is not a valid uri.", uriStr, e);
        throw new IOException(
            String.format("'mvfs.socket' value is invalid. uri: %s", uriStr));
      }
    } else {
      socket = uri.getHost();
    }
    log.info("initialize DMOFileSystem with URI: {}", uri);
    super.initialize(uri, conf);
    workingDir = new Path(uri.getScheme(), uri.getAuthority(), "/");
    this.uri = uri;
    this.conf = conf;
  }

  String getSocket() {
    return socket;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public FSDataOutputStream create(Path path,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    val dmoFile = getDmoFile(path);
    if (dmoFile.exists()) {
      if (overwrite) {
        dmoFile.delete();
      } else {
        throw new FileAlreadyExistsException("File already exists: " + path);
      }
    }
    log.debug("create '{}'.", path.toString());
    dmoFile.create();
    return new FSDataOutputStream(getBufferedOutputStream(dmoFile), statistics);
  }

  private BufferedOutputStream getBufferedOutputStream(DMOFile dmoFile) {
    return new BufferedOutputStream(new DMOOutputStream(dmoFile), BUFFER_SIZE);
  }

  @Override
  public FSDataOutputStream append(
      Path path, int bufferSize, Progressable progress) throws IOException {
    log.debug("write {} bytes to '{}'", bufferSize, path.toString());
    val dmoFile = getDmoFile(path);
    val dmoOs = new DMOOutputStream(dmoFile, true, false);
    return new FSDataOutputStream(dmoOs, statistics);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    val srcFile = getDmoFile(src);
    val dstFile = getDmoFile(dst);
    srcFile.rename(dstFile.getDmoId());
    return true;
  }

  private boolean isDir(Path path) throws IOException {
    return getFileStatus(path).isDirectory();
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    try {
      if (isDir(path)) {
        val children = listStatus(path);
        if (children.length > 0 && !recursive) {
          throw new DirectoryNotEmptyException(makeAbsolute(path).toString());
        }
        log.debug("remove folder '{}'.", path.toString());
        getDmoDir(path).delete();
      } else {
        log.debug("remove file '{}'.", path.toString());
        getDmoFile(path).delete();
      }
    } catch (FileNotFoundException ex) {
      log.info("{} doesn't exists.  Nothing to do.", path);
    }
    return true;
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    final FileStatus[] ret;
    val status = getFileStatus(path);
    if (status.isDirectory()) {
      val idList = getDmoFile(path).list();
      if (idList != null && idList.length > 0) {
        ret = new FileStatus[idList.length];
        for (int i = 0; i < idList.length; i++) {
          val dmoId = idList[i];
          val subPath = new Path(path + "/" + dmoId);
          ret[i] = getFileStatus(subPath);
        }
      } else {
        ret = new FileStatus[0];
      }
    } else {
      ret = new FileStatus[]{status};
    }
    return ret;
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    val dmoFile = getDmoDir(path);
    val exists = dmoFile.exists();
    if (!exists) {
      log.debug("create folder '{}'.", path.toString());
      dmoFile.create();
    }
    val parent = path.getParent();
    return exists || isRoot(parent.toString()) || mkdirs(parent, permission);
  }

  boolean isRoot(String path) {
    boolean ret;
    if (StringUtils.isEmpty(path)) {
      ret = true;
    } else {
      if (path.contains(SCHEMA_HEAD)) {
        path = path.substring(SCHEMA_HEAD.length());
      }
      val stripped = StringUtils.stripEnd(path, "/");
      ret = !stripped.contains("/");
    }
    return ret;
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    val dmoFile = getDmoFile(path);
    final FileStatus ret;
    if (dmoFile.exists()) {
      ret = getFileStatus(dmoFile, path);
    } else {
      val dmoDir = getDmoDir(path);
      if (dmoDir.exists()) {
        ret = getFileStatus(dmoDir, path);
      } else {
        throw new FileNotFoundException(
            String.format("%s not found.", path.toUri().getPath()));
      }
    }

    return ret;
  }

  private FileStatus getFileStatus(DMOFile dmoFile, Path path)
      throws IOException {
    val dmoAttr = dmoFile.attribute();
    return new FileStatus(
        dmoAttr.getSize(),
        !dmoAttr.isFile(),
        dmoAttr.getReplicaCount(),
        getBlockSize(dmoAttr.getChunkSize()),
        dmoAttr.getMTime(),
        dmoAttr.getATime(),
        null,
        null,
        null,
        path.makeQualified(getUri(), getWorkingDirectory()));
  }

  private long getBlockSize(long fileBlockSize) {
    return fileBlockSize < MIN_BLOCK_SIZE ? MIN_BLOCK_SIZE : fileBlockSize;
  }

  DMOFile getDmoFile(Path path) throws IOException {
    val pathStr = getDmoId(path);
    return createDMOFile(pathStr);
  }

  private DMOFile getDmoDir(Path path) throws IOException {
    String dmoId = getDmoId(path);
    if (!dmoId.endsWith("/")) {
      dmoId = dmoId + "/";
    }
    if (!dmoId.startsWith("/")) {
      dmoId = "/" + dmoId;
    }
    return createDMOFile(dmoId);
  }

  private DMOFile createDMOFile(String dmoId) throws IOException {
    return new DMOFile(socket, dmoId);
  }

  private String getDmoId(Path path) {
    String pathStr = makeAbsolute(path).toString();
    if (pathStr.startsWith(SCHEMA_HEAD)) {
      val hostEnd = pathStr.indexOf("/", SCHEMA_HEAD.length());
      pathStr = pathStr.substring(hostEnd);
    }
    return pathStr;
  }


  Path makeAbsolute(Path path) {
    if (workingDir == null) {
      throw new IllegalArgumentException(
          "working dir is null, do you forget to call 'initialize'?");
    }
    return path.isAbsolute() ? path : new Path(workingDir, path);
  }
}

/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package com.memverge.mvfs.connection;

import static com.memverge.mvfs.dmo.DMOFile.ERROR_DIR_NOT_EMPTY;
import static com.memverge.mvfs.dmo.DMOFile.ERROR_FILE_EXISTS;
import static com.memverge.mvfs.dmo.DMOFile.ERROR_FILE_NOT_FOUND;

import com.memverge.mvfs.dmo.DMOFile;
import com.memverge.mvfs.dmo.ObjectAttr;
import com.memverge.mvfs.dmo.PrefetchItem;
import com.memverge.mvfs.utils.TmpFileFolder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;

@Slf4j
public class MockMVFSConnector extends MVFSConnector {

  private static final int DEFAULT_CHUNK_SIZE = 2 * 1024 * 1024;
  private static final int EINVAL = -22;

  private final Map<String, MockDmoAttr> attrMap;
  private final FdManager fdManager = FdManager.getInstance();
  private final TmpFileFolder dmoMockFolder;
  private final Set<String> connected;

  MockMVFSConnector(MVFSConnectOptions options) {
    super(options);
    attrMap = new ConcurrentHashMap<>();
    connected = new HashSet<>();
    dmoMockFolder = new TmpFileFolder("dmoMock", options.getSocket());
  }

  @Override
  protected int connect(MVFSConnectOptions options) {
    final int ret;
    if (options.getSocket().equals("1234")) {
      ret = -19;
    } else {
      connected.add(options.getSocket());
      ret = 0;
    }
    return ret;
  }

  @Override
  protected int disconnect() {
    val path = getOptions().getSocket();
    int rc = 0;
    if (connected.contains(path)) {
      connected.remove(path);
    } else {
      log.error("{} is not connected.", path);
      rc = -1;
    }
    return rc;
  }

  @Override
  public int broadcast(String path) {
    log.info("mock broadcast file {}", path);
    return 0;
  }

  @Override
  public int prefetch(PrefetchItem prefetchItem) {
    log.debug("mock prefetch {}", prefetchItem);
    return 0;
  }

  @Override
  public int create(String path, int chunkSize, int replicas) {
    val file = getFile(path);
    int rc = 0;
    try {
      val folder = file.getParentFile();
      if (!folder.exists()) {
        val success = folder.mkdirs();
        if (!success) {
          log.warn("Create {} failed.", path);
        }
      }
      val created = file.createNewFile();
      if (!created) {
        log.warn("{} already exists.", path);
        rc = DMOFile.ERROR_FILE_EXISTS;
      } else {
        log.debug("{} created with chunk size {}", path, chunkSize);
        val attr = new MockDmoAttr(path, chunkSize, replicas, 0);
        attrMap.put(path, attr);
      }
    } catch (IOException e) {
      log.error("create {} failed.", path, e);
      rc = -1;
    }
    return rc;
  }

  @Override
  public int unlink(String path) {
    val file = getFile(path);
    attrMap.remove(path);
    int ret = 0;
    if (!file.exists()) {
      ret = ERROR_FILE_NOT_FOUND;
    } else {
      try {
        if (file.isDirectory()) {
          if (FileUtils.listFilesAndDirs(
              file,
              TrueFileFilter.INSTANCE,
              DirectoryFileFilter.INSTANCE).size() > 0) {
            log.warn("unlink doesn't allow to remove not empty folder: {}.",
                path);
            ret = ERROR_DIR_NOT_EMPTY;
          } else {
            FileUtils.deleteDirectory(file);
          }
        } else {
          val success = file.delete();
          if (!success) {
            log.warn("remove {} failed.", path);
          }
        }
      } catch (IOException ex) {
        log.info("failed to remove mock dmo: {}", path, ex);
        ret = -1;
      }
    }
    return ret;
  }

  @Override
  public int open(String path, int flags) {
    return openByPath(path);
  }

  private int openByPath(String path) {
    int rc;
    if (!attrMap.containsKey(path) && !getFile(path).exists()) {
      rc = ERROR_FILE_NOT_FOUND;
    } else {
      try {
        val file = new RandomAccessFile(getFile(path), "rw");
        rc = file.getFD().hashCode();
        fdManager.put(rc, path, file);
      } catch (FileNotFoundException e) {
        rc = ERROR_FILE_NOT_FOUND;
      } catch (IOException e) {
        log.error("failed to open file {}", path, e);
        rc = -1;
      }
    }
    return rc;
  }

  @Override
  public ObjectAttr openWithAttr(String path, int flags) {
    int rc = openByPath(path);
    ObjectAttr ret;
    if (rc >= 0) {
      ret = getAttrByPath(path).withRc(rc);
    } else {
      ret = getErrorObjectAttr(rc);
    }
    return ret;
  }

  @Override
  public int close(int fd) {
    val file = fdManager.getFile(fd);
    int rc = 0;
    if (file == null) {
      rc = -1;
    } else {
      try {
        file.close();
        fdManager.remove(fd);
      } catch (IOException e) {
        log.error("failed to close file {}", file, e);
        rc = -1;
      }
    }
    return rc;
  }

  @Override
  protected int _exists(String path) {
    val file = getFile(path);
    return file.exists() ? 0 : ERROR_FILE_NOT_FOUND;
  }

  @Override
  public int mkdir(String path) {
    int rc;
    try {
      val success = getFile(path).mkdirs();
      if (!success) {
        log.warn("mkdir {} failed.", path);
      }
      rc = 0;
    } catch (SecurityException e) {
      log.info("failed to create folder: {}, error: {}", path, e.getMessage());
      rc = -1;
    }
    return rc;
  }

  @Override
  public int rmdir(String path) {
    int rc;
    try {
      val toRemove = getFile(path);
      if (toRemove.exists()) {
        FileUtils.deleteDirectory(toRemove);
      }
      rc = 0;
    } catch (IOException | IllegalArgumentException e) {
      log.info("failed to remove folder: {}, error: {}", path, e.getMessage());
      rc = -1;
    }
    return rc;
  }

  @Override
  public int rename(String srcPath, String dstPath) {
    val dst = getFile(dstPath);
    val src = getFile(srcPath);
    int rc;
    if (dst.exists()) {
      if (dst.isDirectory()) {
        try {
          FileUtils.moveDirectory(src,
              Paths.get(dst.getPath(), src.getName()).toFile());
          rc = 0;
        } catch (IOException e) {
          log.warn("failed to move '{}' to '{}'.", srcPath, dstPath);
          rc = -1;
        }
      } else {
        rc = ERROR_FILE_EXISTS;
      }
    } else if (!src.exists()) {
      rc = ERROR_FILE_NOT_FOUND;
    } else {
      val parentFile = dst.getParentFile();
      if (!parentFile.exists()) {
        val success = parentFile.mkdirs();
        if (!success) {
          log.warn("mkdir {} failed.", parentFile.getAbsolutePath());
        }
      }
      if (src.isFile()) {
        val origAttr = attrMap.get(srcPath);
        if (origAttr != null) {
          attrMap.put(dstPath, origAttr);
          attrMap.remove(srcPath);
        }
      }
      rc = src.renameTo(dst) ? 0 : -1;
    }
    return rc;
  }

  private int read(int fd, byte[] buf, int size, long offset) {
    val file = fdManager.getFile(fd);
    int rc;
    if (size == 0) {
      rc = EINVAL;
    } else {
      try {
        synchronized (fdManager.getFile(fd)) {
          file.seek(offset);
          rc = file.read(buf, 0, size);
        }
      } catch (NullPointerException e) {
        rc = DMOFile.ERROR_STALE_HANDLE;
      } catch (FileNotFoundException e) {
        rc = ERROR_FILE_NOT_FOUND;
      } catch (IOException e) {
        log.error("read {} failed.", file, e);
        rc = -1;
      }
    }
    return rc;
  }

  @Override
  public int read(int fd, byte[] buf, int size, long offset, long bufOffset) {
    int bytesRead;
    if (bufOffset != 0) {
      val tmpBuf = new byte[size];
      bytesRead = read(fd, tmpBuf, size, offset);
      if (bytesRead != -1) {
        bytesRead = (int) Math.min(bytesRead, buf.length - bufOffset);
        System.arraycopy(tmpBuf, 0, buf, (int) bufOffset, bytesRead);
      }
    } else {
      bytesRead = read(fd, buf, size, offset);
    }
    return bytesRead;
  }

  private int write(int fd, byte[] buf, int size, long offset) {
    val file = fdManager.getFile(fd);
    int rc;
    if (size == 0) {
      rc = EINVAL;
    } else {
      try {
        synchronized (fdManager.getFile(fd)) {
          file.seek(offset);
          file.write(buf, 0, size);
        }
        rc = size;
      } catch (IOException e) {
        log.error("write {} failed.", file, e);
        rc = -1;
      }
    }
    return rc;
  }

  @Override
  public int write(int fd, byte[] buf, int size, long offset, long bufOffset) {
    if (bufOffset != 0) {
      size = (int) Math.min(size, buf.length - bufOffset);
      val tmpBuf = new byte[size];
      System.arraycopy(buf, (int) bufOffset, tmpBuf, 0, size);
      buf = tmpBuf;
    }
    return write(fd, buf, size, offset);
  }

  @Override
  public ByteBuffer mmap(String path, long capacity) throws IOException {
    val file = getFile(path);
    ByteBuffer ret;
    if (!file.exists()) {
      throw new FileNotFoundException(path + " not found.");
    } else {
      try (val fileChannel = FileChannel.open(file.toPath())) {
        ret = ByteBuffer.allocateDirect((int) capacity);
        val bytesRead = fileChannel.read(ret);
        ret.flip();
        log.debug("fill {} bytes to buffer from {}",
            bytesRead, file.getAbsolutePath());
      } catch (IOException ex) {
        log.warn("io error in mock, set return value to -1.", ex);
        throw ex;
      }
    }
    return ret;
  }

  @Override
  public ObjectAttr getAttr(String path) {
    return getAttrByPath(path);
  }

  private ObjectAttr getAttrByPath(String path) {
    val mockAttr = attrMap.getOrDefault(path,
        new MockDmoAttr(path, DEFAULT_CHUNK_SIZE, 0, 0));
    val file = getFile(path);
    ObjectAttr ret;
    if (!file.exists()) {
      log.warn("tmp file not found for {}", path);
      ret = getErrorObjectAttr(ERROR_FILE_NOT_FOUND);
    } else {
      try {
        val fileAttr = Files.readAttributes(
            file.toPath(), BasicFileAttributes.class);
        ret = mockAttr.getObjectAttr(fileAttr);
      } catch (IOException e) {
        log.error("read file {} attribute failed.", path, e);
        ret = getErrorObjectAttr(-1);
      }
    }
    return ret;
  }

  @Override
  public int setAttr(String path, ObjectAttr attr) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ObjectAttr getAttr(int fd) {
    ObjectAttr ret;
    if (fdManager.has(fd)) {
      ret = getAttrByPath(fdManager.getPath(fd));
    } else {
      ret = getErrorObjectAttr(ERROR_FILE_NOT_FOUND);
    }
    return ret;
  }

  @Override
  public int setAttr(int fd, ObjectAttr attr) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] listAll() {
    val file = Paths.get(dmoMockFolder.getTmpPath()).toFile();
    return file.exists() ? listAll(file).toArray(new String[0]) : null;
  }

  @Override
  public String[] list(String path) {
    val file = Paths.get(dmoMockFolder.getTmpPath(), path).toFile();
    val ret = new ArrayList<String>();
    val children = file.listFiles();
    if (children != null) {
      for (val child : children) {
        if (child.isDirectory()) {
          ret.add(child.getName() + "/");
        } else {
          ret.add(child.getName());
        }
      }
    }
    return ret.toArray(new String[0]);
  }

  private List<String> listAll(File file) {
    val ret = new ArrayList<String>();
    if (file.exists()) {
      if (file.isDirectory()) {
        val children = file.listFiles();
        if (children != null) {
          val prefix =
              dmoMockFolder.getRelativePath(file.getAbsolutePath()) + "/";
          for (val child : children) {
            if (child.isDirectory()) {
              ret.add(prefix + child.getName() + "/");
              ret.addAll(listAll(child));
            } else {
              ret.add(prefix + child.getName());
            }
          }
        }
      }
    }
    return ret;
  }

  private File getFile(String path) {
    path = path.replaceAll("[^a-z\t A-Z0-9`/\\-$_.]", "_");
    return Paths.get(dmoMockFolder.getTmpPath(), path).toFile();
  }

  private ObjectAttr getErrorObjectAttr(int rc) {
    return new ObjectAttr(rc, 0, 0, 0, true, 0, 0, 0);
  }
}

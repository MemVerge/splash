/*
 * Copyright (c) 2018 MemVerge Inc.
 */

package com.memverge.mvfs.dmo;

import com.memverge.mvfs.error.MVFSException;
import java.io.IOException;
import java.io.OutputStream;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class DMOOutputStream extends OutputStream {

  private final DMOFile dmoFile;
  private final boolean append;
  private final boolean create;
  private int fd;
  private volatile boolean closed;
  private final Object closeLock;
  // offset used by single byte write
  private long offset;

  public DMOOutputStream(DMOFile dmoFile) {
    this(dmoFile, false);
  }

  public DMOOutputStream(DMOFile dmoFile, boolean append) {
    this(dmoFile, append, false);
  }

  public DMOOutputStream(DMOFile dmoFile, boolean append, boolean create) {
    this.closeLock = new Object();
    this.dmoFile = dmoFile;
    this.append = append;
    this.create = create;
    this.fd = -1;
    this.closed = true;
    this.offset = -1;
  }

  private void open() throws IOException {
    ObjectAttr attr;
    synchronized (closeLock) {
      if (!closed) {
        return;
      }
      attr = dmoFile.open(create);
      fd = attr.getRc();
      closed = false;
    }
    if (append) {
      offset = attr.getSize();
    } else if (offset < 0) {
      offset = 0;
    }
  }

  /**
   * Write byte (lowest byte of the "i") to the file.
   */
  @Override
  public void write(int i) throws IOException {
    val bytes = new byte[1];
    bytes[0] = (byte) i;
    write(bytes, 0L, 1);
  }

  @Override
  public void write(byte[] bytes) throws IOException {
    write(bytes, 0L, bytes.length);
  }

  @Override
  public void write(byte[] bytes, int bufOffset, int len) throws IOException {
    write(bytes, (long) bufOffset, len);
  }

  public void write(byte[] bytes, long bufOffset, int len) throws IOException {
    if (closed) {
      open();
    }

    final int written;
    if (len == 0) {
      written = 0;
    } else {
      written = dmoFile.getMvfsConn().write(fd, bytes, len, offset, bufOffset);
    }

    if (written < 0) {
      throw new MVFSException("mvfs_write() failed.", dmoFile, written);
    } else if (written != len) {
      val msg = String.format("mvfs_write() failed. %d bytes written while "
          + "%d bytes expected.", written, len);
      throw new MVFSException(msg, dmoFile);
    } else {
      offset += written;
    }
  }

  @Override
  public void flush() {
    // no extra things to do
  }

  @Override
  public void close() throws IOException {
    synchronized (closeLock) {
      if (closed) {
        return;
      }
      closed = true;
    }
    dmoFile.close(fd);
    this.offset = -1;
  }

  public long position() throws IOException {
    if (closed) {
      open();
    }
    return offset;
  }
}

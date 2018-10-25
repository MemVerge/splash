/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package com.memverge.mvfs.dmo;

import com.memverge.mvfs.error.MVFSException;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class DMOInputStream extends InputStream {

  private static long NOT_SET = -1;

  private final DMOFile dmoFile;
  private long endOffsetExclusive = NOT_SET;
  @Getter
  private long offset = 0;

  public DMOInputStream(DMOFile dmoFile) {
    this.dmoFile = dmoFile;
  }

  public DMOInputStream(DMOFile dmoFile, long endOffsetExclusive) {
    this(dmoFile);
    if (endOffsetExclusive < 0) {
      log.warn("Invalid endOffsetExclusive {} supplied, default to -1.",
          endOffsetExclusive);
      endOffsetExclusive = NOT_SET;
    }
    this.endOffsetExclusive = endOffsetExclusive;
  }

  /**
   * read 1 byte to int.
   */
  @Override
  public int read() throws IOException {
    val bytes = new byte[1];
    val rc = read(bytes, 0L, 1);
    int ret;
    if (rc >= 0) {
      // convert byte to an unsigned int
      ret = bytes[0] & 0xFF;
    } else {
      ret = -1;
    }
    return ret;
  }

  @Override
  public int read(byte[] bytes) throws IOException {
    return read(bytes, 0L, bytes.length);
  }

  @Override
  public int read(byte[] bytes, int bufOffset, int len) throws IOException {
    return read(bytes, (long) bufOffset, len);
  }

  public int read(byte bytes[], long bufOffset, int len) throws IOException {
    int bytesRead = -1;
    val lenToRead = getActualLenToRead(len);
    val endOfFile = offset == getSize();
    if (lenToRead == 0 || endOfFile) {
      bytesRead = 0;
    } else {
      int tryCount = 1;
      while (tryCount <= 10) {
        tryCount++;
        bytesRead = dmoFile.getMvfsConn().read(
            getFd(), bytes, lenToRead, offset, bufOffset);
        if (bytesRead == DMOFile.ERROR_STALE_HANDLE) {
          FileHandleCache.getInstance().invalidate(dmoFile);
          log.info("file handle for {} in cache is invalid.  retry {}",
              dmoFile.getDmoId(), tryCount);
        } else {
          break;
        }
      }
    }

    if (bytesRead < -1) {
      throw new MVFSException("mvfs_read() failed.", dmoFile, bytesRead);
    } else if (bytesRead > 0) {
      offset += bytesRead;
    } else {
      // bytesRead == 0 means end of file
      // change it to -1 to align with Java API
      bytesRead = -1;
    }
    return bytesRead;
  }

  private int getActualLenToRead(int len) {
    if (endOffsetExclusive != NOT_SET && offset + len > endOffsetExclusive) {
      len = (int) (endOffsetExclusive - offset);
      if (len < 0) {
        len = 0;
      }
    }
    return len;
  }

  /**
   * skip bytes to read, n could be negative indicate backwards.
   *
   * @return actual bytes skipped.
   */
  public long skip(long n) {
    if (n < 0) {
      if (n + offset < 0) {
        n = -offset;
        offset = 0;
      } else {
        offset += n;
      }
    } else {
      val total = getSize();
      if (n + offset >= total) {
        n = total - offset;
        offset = total;
      } else {
        offset += n;
      }
    }
    return n;
  }

  long getSize() {
    long ret = -1;
    try {
      ret = FileHandleCache.getInstance().getSize(dmoFile);
      if (endOffsetExclusive != NOT_SET && ret > endOffsetExclusive + 1) {
        ret = endOffsetExclusive;
      }
    } catch (IOException e) {
      log.error("failed to retrieve size of {}.", dmoFile.getDmoId(), e);
    }
    return ret;
  }

  public int available() {
    val total = getSize();
    val remain = total - offset;
    return (int) (remain < 0 ? 0 : remain);
  }

  public long seek(long offset) throws IOException {
    if (offset < 0) {
      this.offset = 0;
    } else {
      long size = getSize();
      if (offset > size) {
        val msg = String.format("End of '%s' reached.  offset: %d, size: %d.",
            dmoFile.getDmoId(), offset, size);
        throw new EOFException(msg);
      } else {
        this.offset = offset;
      }
    }
    return this.offset;
  }

  public void prefetch() throws IOException {
    // prefetch what's left
    dmoFile.prefetch(offset, available());
  }

  int getFd() throws IOException {
    val fd = FileHandleCache.getInstance().getReadOnlyFd(dmoFile);
    if (fd < 0) {
      throw new FileNotFoundException(
          String.format("%s not found.", dmoFile.getDmoId()));
    }
    return fd;
  }
}

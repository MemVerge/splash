/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.hdfs;

import com.memverge.mvfs.dmo.DMOFile;
import com.memverge.mvfs.dmo.DMOInputStream;
import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

@Slf4j
public class HdfsDmoInputStream extends FSInputStream
    implements Seekable, PositionedReadable {

  private static final int DEFAULT_BUFFER_SIZE = 8 * 1024;
  static final int MAX_BUFFER_SIZE = 2 * 1024 * 1024;

  private final Statistics stats;
  private final DMOFile dmoFile;
  private boolean alreadyClosed;

  @Getter
  private final int bufferSize;
  private long offset;

  private BufferedInputStream bis = null;

  HdfsDmoInputStream(DMOFile dmoFile, Statistics stats) {
    this(dmoFile, stats, DEFAULT_BUFFER_SIZE);
  }

  HdfsDmoInputStream(DMOFile dmoFile, Statistics stats, int bufferSize) {
    this.alreadyClosed = false;
    this.dmoFile = dmoFile;
    this.stats = stats;
    this.offset = 0;
    this.bufferSize = bufferSize > MAX_BUFFER_SIZE ? MAX_BUFFER_SIZE : bufferSize;
    log.debug("create HDFS DMO input stream for {}, buffer size {}.",
        dmoFile.getDmoId(), this.bufferSize);
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
    }
    val prevOffset = offset;
    if (pos > offset) {
      // if we already read, bufferInputStream will only skip left buffer
      // use a while loop to ensure we reach the correct offset
      // no actual read happens no there is no IO overhead
      val bufferedIs = getBis();
      while (pos != offset) {
        val skipped = bufferedIs.skip(pos - offset);
        if (skipped == 0) {
          break;
        }
        offset += skipped;
      }
      log.debug("seek to {} offset {}, prev offset {}, skipped {}.",
          dmoFile.getDmoId(), pos, prevOffset, offset);
      if (offset < pos) {
        log.warn("seek to {} while size of {} is {}.",
            pos, dmoFile.getDmoId(), dmoFile.getSize());
        throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
      }
    } else if (pos == offset) {
      log.debug("seek to {} offset {}, prev offset {}, nothing to do.",
          dmoFile.getDmoId(), offset, prevOffset);
    } else {
      log.debug("seek to {}, offset {}, seek backwards.  Reopen input stream.",
          dmoFile.getDmoId(), offset);
      closeBufferedInputStream();
      seek(pos);
    }
  }

  private BufferedInputStream getBis() throws IOException {
    if (bis == null) {
      if (alreadyClosed) {
        throw new IOException("Attempt to read from closed stream.");
      }
      log.debug("create buffered cache for {}", dmoFile.getDmoId());
      bis = new BufferedInputStream(new DMOInputStream(dmoFile), bufferSize);
    }
    return bis;
  }

  @Override
  public long getPos() {
    return offset;
  }

  @Override
  public boolean seekToNewSource(long targetPos) {
    return false;
  }

  @Override
  public int read() throws IOException {
    val byteRead = getBis().read();
    offset += 1;
    if (stats != null && byteRead >= 0) {
      stats.incrementBytesRead(1);
    }
    return byteRead;
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    log.trace("read {} bytes at offset {} from '{}'.", len, offset, dmoFile);
    val bytesRead = getBis().read(buf, off, len);
    offset += bytesRead;
    if (stats != null && bytesRead > 0) {
      stats.incrementBytesRead(bytesRead);
    }
    return bytesRead;
  }

  private void closeBufferedInputStream() throws IOException {
    if (bis != null) {
      bis.close();
    }
    offset = 0;
    bis = null;
  }

  @Override
  public void close() throws IOException {
    closeBufferedInputStream();
    alreadyClosed = true;
  }
}

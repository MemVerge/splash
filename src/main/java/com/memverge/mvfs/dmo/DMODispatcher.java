/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.dmo;

import java.io.IOException;
import java.nio.ByteBuffer;
import lombok.Data;
import lombok.val;

@Data
class DMODispatcher {

  private final DMOFile dmoFile;
  private final int fd;
  private long offset = 0L;

  public static DMODispatcher create(DMOFile dmoFile, int fd) {
    return new DMODispatcher(dmoFile, fd);
  }

  String getSocket() {
    return dmoFile.getSocketPath();
  }

  String getDmoId() {
    return dmoFile.getDmoId();
  }

  long getSize() {
    return dmoFile.getSize(fd);
  }

  void close() throws IOException {
    dmoFile.close(fd);
  }

  int read(ByteBuffer byteBuffer, int bufOffset) throws IOException {
    val buf = byteBuffer.array();
    val length = byteBuffer.limit() - byteBuffer.position();
    val bytesRead = dmoFile.getMvfsConn().read(
        fd, buf, length, offset, bufOffset);
    if (bytesRead >= 0) {
      offset += bytesRead;
    }
    return bytesRead;
  }

  int write(ByteBuffer byteBuffer, int bufOffset) throws IOException {
    val buf = byteBuffer.array();
    val length = byteBuffer.limit() - byteBuffer.position();
    val bytesWritten = dmoFile.getMvfsConn().write(
        fd, buf, length, offset, bufOffset);
    if (bytesWritten >= 0) {
      offset += bytesWritten;
    }
    return bytesWritten;
  }

  void position(long newPosition) {
    offset = newPosition;
  }

  long position() {
    return offset;
  }
}

/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.dmo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.util.Arrays;
import lombok.Synchronized;
import lombok.val;
import lombok.var;

public class DMOChannel extends AbstractInterruptibleChannel
    implements SeekableByteChannel, GatheringByteChannel,
    ScatteringByteChannel {

  private final DMODispatcher dmoDispatcher;
  private final boolean readable;
  private final boolean writable;

  private DMOChannel(DMOFile dmoFile,
      int fd,
      boolean readable,
      boolean writable,
      boolean append) {
    this.dmoDispatcher = DMODispatcher.create(dmoFile, fd);
    if (append) {
      dmoDispatcher.position(dmoDispatcher.getSize());
    }
    this.readable = readable;
    this.writable = writable;
  }

  public static DMOChannel open(DMOFile dmoFile)
      throws IOException {
    val fd = dmoFile.open(true).getRc();
    return new DMOChannel(dmoFile, fd, true, true, false);
  }

  public static DMOChannel openForRead(DMOFile dmoFile) throws IOException {
    val fd = dmoFile.open(false).getRc();
    return new DMOChannel(dmoFile, fd, true, false, false);
  }

  public static DMOChannel openForWrite(DMOFile dmoFile, boolean append)
      throws IOException {
    val fd = dmoFile.open(true).getRc();
    return new DMOChannel(dmoFile, fd, false, true, append);
  }

  public static DMOChannel openForWrite(DMOFile dmoFile) throws IOException {
    return openForWrite(dmoFile, true);
  }

  @Override
  public long write(ByteBuffer[] srcs, int offset, int length)
      throws IOException {
    ByteBuffer[] toWrite;
    if (offset != 0) {
      val to = Math.min(srcs.length, offset + length);
      toWrite = Arrays.copyOfRange(srcs, offset, to);
    } else {
      toWrite = srcs;
    }

    return doWrite(toWrite);
  }

  @Override
  public long write(ByteBuffer[] srcs) throws IOException {
    return write(srcs, 0, srcs.length);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    ensureOpen();
    ensureWritable();
    return doWrite(src);
  }

  public int write(String src) throws IOException {
    return write(ByteBuffer.wrap(src.getBytes()));
  }

  @Synchronized
  private int doWrite(ByteBuffer[] srcs) throws IOException {
    int ret = 0;
    try {
      begin();
      if (isOpen()) {
        for (val src : srcs) {
          ret += doWriteBuffer(src);
        }
      }
    } finally {
      complete();
    }
    return ret;
  }

  @Synchronized
  private int doWrite(ByteBuffer src) throws IOException {
    int ret = 0;
    try {
      begin();
      if (isOpen()) {
        ret = doWriteBuffer(src);
      }
    } finally {
      complete();
    }
    return ret;
  }

  private int doWriteBuffer(ByteBuffer dst) throws IOException {
    val bytesWritten = dmoDispatcher.write(dst, dst.position());
    if (bytesWritten > 0) {
      dst.position(dst.position() + bytesWritten);
    }
    return bytesWritten;
  }

  @Override
  public long read(ByteBuffer[] dsts, int offset, int length)
      throws IOException {
    ByteBuffer[] toFill;
    if (offset != 0) {
      val to = Math.min(dsts.length, offset + length);
      toFill = Arrays.copyOfRange(dsts, offset, to);
    } else {
      toFill = dsts;
    }

    return doRead(toFill);
  }

  @Override
  public long read(ByteBuffer[] dsts) throws IOException {
    return read(dsts, 0, dsts.length);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    ensureOpen();
    ensureReadable();
    return doRead(dst);
  }

  @Synchronized
  private int doRead(ByteBuffer[] dsts) throws IOException {
    int ret = 0;
    try {
      begin();
      if (isOpen()) {
        for (val dst : dsts) {
          ret += doReadBuffer(dst);
        }
      }
    } finally {
      complete();
    }
    return ret;
  }

  @Synchronized
  private int doRead(ByteBuffer dst) throws IOException {
    int ret = 0;
    try {
      begin();
      if (isOpen()) {
        ret = doReadBuffer(dst);
      }
    } finally {
      complete();
    }
    return ret;
  }

  private int doReadBuffer(ByteBuffer dst) throws IOException {
    ensureBufferIsNotReadOnly(dst);
    val bytesRead = dmoDispatcher.read(dst, dst.position());
    if (bytesRead > 0) {
      dst.position(dst.position() + bytesRead);
    }
    return bytesRead;
  }

  private void ensureBufferIsNotReadOnly(ByteBuffer dst) {
    if (dst.isReadOnly()) {
      throw new IllegalArgumentException("Read-only buffer");
    }
  }

  @Override
  public long position() {
    return dmoDispatcher.position();
  }

  @Override
  public DMOChannel position(long newPosition) throws IOException {
    ensureOpen();
    if (newPosition < 0) {
      val msg = String.format("position %d is negative.", newPosition);
      throw new IllegalArgumentException(msg);
    }
    DMOChannel ret;
    if (doPosition(newPosition)) {
      ret = this;
    } else {
      ret = null;
    }
    return ret;
  }

  @Synchronized
  private boolean doPosition(long newPosition) throws IOException {
    var success = false;
    try {
      begin();
      if (isOpen()) {
        dmoDispatcher.position(newPosition);
        success = true;
      }
    } finally {
      complete();
    }
    return success;
  }

  @Override
  public long size() throws IOException {
    ensureOpen();
    return dmoDispatcher.getSize();
  }

  @Override
  public SeekableByteChannel truncate(long size) {
    throw new UnsupportedOperationException(
        "Truncate for dmo has not been implemented.");
  }

  @Override
  protected void implCloseChannel() throws IOException {
    dmoDispatcher.close();
  }

  public String getSocket() {
    return dmoDispatcher.getSocket();
  }

  public String getDmoId() {
    return dmoDispatcher.getDmoId();
  }

  private void complete() throws AsynchronousCloseException {
    end(true);
  }

  private void ensureOpen() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
  }

  private void ensureWritable() {
    if (!writable) {
      throw new NonWritableChannelException();
    }
  }

  private void ensureReadable() {
    if (!readable) {
      throw new NonReadableChannelException();
    }
  }
}

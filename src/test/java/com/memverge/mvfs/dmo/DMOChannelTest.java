/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.dmo;

import static com.memverge.mvfs.TestGroups.IT;
import static com.memverge.mvfs.TestGroups.UT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = {UT, IT})
public class DMOChannelTest {

  private static final String content = "Blue Whale";
  private DMOChannel channel = null;
  private DMOFile file = null;

  @BeforeMethod
  private void beforeMethod() throws IOException {
    afterMethod();

    val dmoId = String.format("dmoChannelTest-%s", UUID.randomUUID());
    file = new DMOFile(dmoId);
    log.debug("create test dmo file: {}", dmoId);
    val buffer = ByteBuffer.wrap(content.getBytes());
    val writeChannel = DMOChannel.openForWrite(file);
    writeChannel.write(buffer);
    writeChannel.close();
  }

  @AfterMethod
  private void afterMethod() throws IOException {
    if (channel != null) {
      channel.close();
    }
    if (file != null) {
      file.forceDelete();
    }

    // used by testFileBehavior
    FileUtils.deleteQuietly(new File("whale"));
  }

  @AfterClass
  private void afterClass() {
    DMOFile.safeReset();
  }

  public void testReadToHeapBuffer() throws IOException {
    val buffer = ByteBuffer.allocate(20);
    channel = DMOChannel.openForRead(file);
    val bytesRead = channel.read(buffer);
    val str = new String(buffer.array());

    assertThat(bytesRead).isEqualTo(10);
    assertThat(str).startsWith(content);
  }

  public void testDoubleRead() throws IOException {
    val buffer = ByteBuffer.allocate(20);
    channel = DMOChannel.openForRead(file);
    var bytesRead = channel.read(buffer);
    channel.position(0);
    bytesRead += channel.read(buffer);
    val str = new String(buffer.array());

    assertThat(buffer.position()).isEqualTo(20);
    assertThat(bytesRead).isEqualTo(20);
    assertThat(str).isEqualTo(content + content);
  }

  public void testReadBufferArray() throws IOException {
    val buffer0 = ByteBuffer.allocate(4);
    val buffer1 = ByteBuffer.allocate(1);
    val buffer2 = ByteBuffer.allocate(10);
    val dsts = new ByteBuffer[]{buffer0, buffer1, buffer2};
    channel = DMOChannel.openForRead(file);
    val bytesRead = channel.read(dsts);

    assertThat(bytesRead).isEqualTo(10);
    assertThat(new String(buffer0.array())).endsWith("Blue");
    assertThat(new String(buffer1.array())).isEqualTo(" ");
    assertThat(new String(buffer2.array())).startsWith("Whale");
    assertThat(buffer2.position()).isEqualTo(5);
  }

  public void testReadBufferArrayWithOffset() throws IOException {
    val buffer0 = ByteBuffer.allocate(100);
    val buffer1 = ByteBuffer.allocate(10);
    val buffer2 = ByteBuffer.allocate(1);
    val buffer3 = ByteBuffer.allocate(5);
    val buffer4 = ByteBuffer.allocate(20);

    buffer1.position(6);
    val dsts = new ByteBuffer[]{buffer0, buffer1, buffer2, buffer3, buffer4};
    channel = DMOChannel.openForRead(file);
    val bytesRead = channel.read(dsts, 1, 3);

    assertThat(bytesRead).isEqualTo(10);
    assertThat(buffer0.position()).isEqualTo(0);
    assertThat(buffer1.position()).isEqualTo(10);
    assertThat(new String(buffer1.array())).endsWith("Blue");
    assertThat(new String(buffer2.array())).isEqualTo(" ");
    assertThat(new String(buffer3.array())).isEqualTo("Whale");
    assertThat(buffer4.position()).isEqualTo(0);
  }

  public void testReadWithLimit() throws IOException {
    val buffer = ByteBuffer.allocate(10);
    buffer.limit(4);
    channel = DMOChannel.openForRead(file);
    val bytesRead = channel.read(buffer);
    val str = new String(buffer.array());

    assertThat(bytesRead).isEqualTo(4);
    assertThat(str).startsWith("Blue");
    assertThat(str).doesNotContain(" ");
  }

  public void testReadClosedChannel() throws IOException {
    channel = DMOChannel.openForRead(file);
    channel.close();
    assertThatExceptionOfType(ClosedChannelException.class)
        .isThrownBy(() -> channel.read(ByteBuffer.allocate(10)));
  }

  public void testNotReadableChannel() throws IOException {
    channel = DMOChannel.openForWrite(file);
    assertThatExceptionOfType(NonReadableChannelException.class)
        .isThrownBy(() -> channel.read(ByteBuffer.allocate(10)));
  }

  public void testWriteClosedChannel() throws IOException {
    channel = DMOChannel.openForWrite(file);
    channel.close();
    assertThatExceptionOfType(ClosedChannelException.class)
        .isThrownBy(() -> channel.write("more"));
  }

  public void testNotWritableChannel() throws IOException {
    channel = DMOChannel.openForRead(file);
    assertThatExceptionOfType(NonWritableChannelException.class)
        .isThrownBy(() -> channel.write("more"));
  }

  public void testMultipleWrite() throws IOException {
    val writeChannel = DMOChannel.openForWrite(file, true);
    writeChannel.write(" is");
    writeChannel.write(" here");
    writeChannel.close();

    channel = DMOChannel.openForRead(file);
    val buffer = ByteBuffer.allocate(20);
    val bytesRead = channel.read(buffer);
    val str = new String(buffer.array());

    assertThat(buffer.position()).isEqualTo(18);
    assertThat(bytesRead).isEqualTo(18);
    assertThat(str).startsWith(content + " is here");
  }

  public void testPosition() throws IOException {
    val buffer = ByteBuffer.allocate(20);
    channel = DMOChannel.openForRead(file).position(5);
    val bytesRead = channel.read(buffer);
    val str = new String(buffer.array());

    assertThat(bytesRead).isEqualTo(5);
    assertThat(str).startsWith("Whale");
  }

  public void testNegativePosition() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> {
          channel = DMOChannel.openForRead(file);
          channel.position(-1);
        });
  }

  public void testWriteBufferArray() throws IOException {
    val buffer0 = ByteBuffer.wrap(" is".getBytes());
    val buffer1 = ByteBuffer.wrap(" very".getBytes());
    val buffer2 = ByteBuffer.wrap(" happy".getBytes());
    val srcs = new ByteBuffer[]{buffer0, buffer1, buffer2};
    channel = DMOChannel.openForWrite(file);
    val bytesWritten = channel.write(srcs);
    channel.close();

    val buffer = ByteBuffer.allocate(24);
    channel = DMOChannel.openForRead(file);
    channel.read(buffer);

    assertThat(bytesWritten).isEqualTo(14);
    assertThat(buffer0.position()).isEqualTo(3);
    assertThat(new String(buffer.array()))
        .isEqualTo(content + " is very happy");
    assertThat(buffer.position()).isEqualTo(24);
  }

  public void testWriteBufferArrayWithIndex() throws IOException {
    val buffer0 = ByteBuffer.wrap(" is not".getBytes());
    val buffer1 = ByteBuffer.wrap(" very happy".getBytes());
    val buffer2 = ByteBuffer.wrap("?!!!".getBytes());
    val buffer3 = ByteBuffer.wrap("sad".getBytes());
    buffer1.position(5);
    buffer2.position(1);
    val srcs = new ByteBuffer[]{buffer0, buffer1, buffer2, buffer3};
    channel = DMOChannel.openForWrite(file);
    val bytesWritten = channel.write(srcs, 1, 2);
    channel.close();

    val buffer = ByteBuffer.allocate(17);
    channel = DMOChannel.openForRead(file);
    val bytesRead = channel.read(buffer);

    assertThat(bytesWritten).isEqualTo(9); // written " happy!!!"
    assertThat(buffer0.position()).isEqualTo(0);
    assertThat(buffer1.position()).isEqualTo(11);
    assertThat(bytesRead).isEqualTo(17);
    assertThat(new String(buffer.array()))
        .isEqualTo(content + " happy!");
    assertThat(buffer.position()).isEqualTo(17);
  }

  public void testWriteWithLimit() throws IOException {
    val toWrite = ByteBuffer.wrap(" is huge or not?".getBytes());
    toWrite.limit(8);
    channel = DMOChannel.openForWrite(file);
    val bytesWritten = channel.write(toWrite);
    channel.close();

    val buffer = ByteBuffer.allocate(100);
    channel = DMOChannel.openForRead(file);
    val bytesRead = channel.read(buffer);
    val str = new String(buffer.array());

    assertThat(bytesWritten).isEqualTo(8);
    assertThat(bytesRead).isEqualTo(18);
    assertThat(str).startsWith("Blue Whale is huge");
    assertThat(str).doesNotContain("or not");
  }

  // use to verify the behavior in FileChannel
  public void testFileBehavior() throws IOException {
    val buffer = ByteBuffer.wrap(content.getBytes());
    val plainFile = Paths.get("whale");
    val writeChannel = FileChannel.open(plainFile,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE_NEW);
    writeChannel.write(buffer);
    writeChannel.close();

    val readBuffer = ByteBuffer.allocate(20);
    val readChannel = FileChannel.open(plainFile, StandardOpenOption.READ);
    readChannel.read(readBuffer);
    readChannel.position(0);
    readChannel.read(readBuffer);
    val str = new String(readBuffer.array());
    assertThat(str).isEqualTo(content + content);
  }
}

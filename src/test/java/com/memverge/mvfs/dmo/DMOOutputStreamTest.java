/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package com.memverge.mvfs.dmo;

import static com.memverge.mvfs.TestGroups.IT;
import static com.memverge.mvfs.TestGroups.UT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import lombok.val;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = {UT, IT})
public class DMOOutputStreamTest {

  @AfterMethod
  private void afterMethod() {
    DMOFile.safeReset();
  }

  private DMOFile getTestDmoFile() throws IOException {
    return new DMOFile("DMOOutputStreamTestFile");
  }

  public void testWriteUpdatesOffset() throws IOException {
    val dmoFile = getTestDmoFile();
    try (val dmoOs = new DMOOutputStream(dmoFile, false, true)) {
      dmoOs.write("012".getBytes());
      dmoOs.write("345".getBytes());
      dmoOs.flush();
    }
    try (val dmoIs = new DMOInputStream(dmoFile)) {
      val buf = new byte[6];
      assertThat(dmoIs.read(buf)).isEqualTo(6);
      assertThat(new String(buf)).isEqualTo("012345");
    }
  }

  public void testPosition() throws IOException {
    val dmoFile = getTestDmoFile();
    try (val dmoOs = new DMOOutputStream(dmoFile, false, true)) {
      dmoOs.write("012".getBytes());
      assertThat(dmoOs.position()).isEqualTo(3);
    }
    try (val dmoOs = new DMOOutputStream(dmoFile, true)) {
      assertThat(dmoOs.position()).isEqualTo(3);
      dmoOs.write("345".getBytes());
      assertThat(dmoOs.position()).isEqualTo(6);
    }
  }

  public void testWriteMultipleTimes() throws IOException {
    val dmoFile = getTestDmoFile();
    try (val dmoOs = new DMOOutputStream(dmoFile, false, true)) {
      dmoOs.write("01234".getBytes(), 2, 2);
      dmoOs.write("56789".getBytes(), 3, 2);
      dmoOs.flush();
    }
    try (val dmoIs = new DMOInputStream(dmoFile)) {
      val buf = new byte[4];
      assertThat(dmoIs.read(buf)).isEqualTo(4);
      assertThat(new String(buf)).isEqualTo("2389");
    }
  }

  public void testOverwrite() throws IOException {
    val dmoFile = getTestDmoFile();
    try (val dmoOs = new DMOOutputStream(dmoFile, false, true)) {
      dmoOs.write("01234".getBytes());
    }

    try (val dmoOs = new DMOOutputStream(dmoFile, false, true)) {
      dmoOs.write("56789".getBytes(), 3, 2);
    }

    try (val dmoIs = new DMOInputStream(dmoFile)) {
      val buf = new byte[4];
      assertThat(dmoIs.read(buf)).isEqualTo(4);
      assertThat(new String(buf)).isEqualTo("8923");
    }
  }

  public void testSingleByteWrite() throws IOException {
    val dmoFile = getTestDmoFile();
    try (val dmoOs = new DMOOutputStream(dmoFile, false, true)) {
      for (val c : "123xyz".getBytes()) {
        dmoOs.write((int) c);
      }
    }

    try (val dmoIs = new DMOInputStream(dmoFile)) {
      val buf = new byte[10];
      assertThat(dmoIs.read(buf)).isEqualTo(6);
      assertThat(new String(buf).substring(0, 6)).isEqualTo("123xyz");
    }
  }

  @Test(enabled=false)
  public void testMultipleFlush() throws IOException {
    val dmoFile = getTestDmoFile();
    try (val dmoOs = new DMOOutputStream(dmoFile, false, true)) {
      dmoOs.write("012".getBytes());
      dmoOs.flush();
    }

    try (val dmoIs = new DMOInputStream(dmoFile)) {
      val buf = new byte[3];
      assertThat(dmoIs.read(buf)).isEqualTo(3);
      assertThat(new String(buf)).isEqualTo("012");
    }

    try (val dmoOs = new DMOOutputStream(dmoFile, true, false)) {
      dmoOs.write("345".getBytes());
      dmoOs.flush();
    }

    try (val dmoIs = new DMOInputStream(dmoFile)) {
      dmoIs.seek(3);
      val buf = new byte[3];
      assertThat(dmoIs.read(buf)).isEqualTo(3);
      assertThat(new String(buf)).isEqualTo("345");
    }

    try (val dmoIs = new DMOInputStream(dmoFile)) {
      val buf = new byte[6];
      assertThat(dmoIs.read(buf)).isEqualTo(6);
      assertThat(new String(buf)).isEqualTo("012345");
    }
  }

  public void testWriteZeroLen() throws IOException {
    val dmoFile = getTestDmoFile();
    try (val dmoOs = new DMOOutputStream(dmoFile, false, true)) {
      dmoOs.write(5);
      dmoOs.write(new byte[0], 0, 0);
      dmoOs.write(10);
    }

    try (val dmoIs = new DMOInputStream(dmoFile)) {
      val buf = new byte[2];
      assertThat(dmoIs.read(buf)).isEqualTo(2);
      assertThat(buf[0]).isEqualTo((byte)5);
      assertThat(buf[1]).isEqualTo((byte)10);
    }
  }
}

/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package com.memverge.mvfs.dmo;

import static com.memverge.mvfs.TestGroups.IT;
import static com.memverge.mvfs.TestGroups.UT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import lombok.val;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = {UT, IT})
public class DMOInputOutputStreamTest {
  @AfterMethod
  private void afterMethod() {
    DMOFile.safeReset();
  }

  public void testReaderAndWriter() throws IOException {
    val file = new DMOFile("testReaderAndWriter");
    val str = "read/write with writer.";
    val len = str.length();
    try (val os = new OutputStreamWriter(new DMOOutputStream(file, false, true));
        val is = new InputStreamReader(new DMOInputStream(file))) {
      os.write(str);
      os.flush();
      val buf = new char[len];
      assertThat(is.read(buf)).isEqualTo(len);
      assertThat(String.valueOf(buf)).isEqualTo(str);
    }
  }

  public void testReadWriteByBuff() throws IOException {
    val file = new DMOFile("testReadWriteByBuff");
    val str = "read/write by buffer.";
    val len = str.length();
    try (val os = new DMOOutputStream(file, false, true);
        val is = new DMOInputStream(file)) {
      os.write(str.getBytes());
      os.flush();
      val buf = new byte[len];
      assertThat(is.read(buf)).isEqualTo(len);
      assertThat(new String(buf)).isEqualTo(str);
    }
  }

  public void testSingleByteReadWrite() throws IOException {
    val file = new DMOFile("singleByteReadWrite");
    try (val os = new DMOOutputStream(file, false, true)) {
      os.write((int) 'a');
      os.write((int) 'b');
      os.flush();
    }
    try (val is = new DMOInputStream(file)) {
      assertThat((char) is.read()).isEqualTo('a');
      assertThat((char) is.read()).isEqualTo('b');
    }
  }
}

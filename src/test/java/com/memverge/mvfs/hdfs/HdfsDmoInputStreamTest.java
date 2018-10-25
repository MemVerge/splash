package com.memverge.mvfs.hdfs;

import static com.memverge.mvfs.TestGroups.IT;
import static com.memverge.mvfs.TestGroups.UT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.memverge.mvfs.dmo.DMOFile;
import com.memverge.mvfs.dmo.DMOOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.util.zip.CRC32;
import lombok.val;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {UT, IT})
public class HdfsDmoInputStreamTest {

  private static final String content = "ABCDEFGHIJKLMN";
  private DMOFileSystem dmoFs;
  private HdfsTestUtil testUtil;

  private DMOFile getTestFile() throws IOException {
    return new DMOFile("HdfsDmoInputStreamTest");
  }

  @BeforeMethod
  private void beforeMethod() throws IOException, URISyntaxException {
    val dmoFile = getTestFile();
    try (val osw = new OutputStreamWriter(
        new DMOOutputStream(dmoFile, false, true))) {
      osw.write(content);
    }
    dmoFs = HdfsTestUtil.createDmoFs();
    testUtil = new HdfsTestUtil(dmoFs);
  }

  @AfterMethod
  private void afterMethod() {
    DMOFile.safeReset();
  }

  public void testGetOffset() throws IOException {
    try (val hdfsIs = new HdfsDmoInputStream(getTestFile(), null)) {
      val buf = new byte[3];
      assertThat(hdfsIs.read(buf)).isEqualTo(3);
      assertThat(new String(buf)).isEqualTo("ABC");
      assertThat(hdfsIs.getPos()).isEqualTo(3L);

      assertThat((char) hdfsIs.read()).isEqualTo('D');
      assertThat(hdfsIs.getPos()).isEqualTo(4L);

      assertThat(hdfsIs.read(buf)).isEqualTo(3);
      assertThat(new String(buf)).isEqualTo("EFG");
      assertThat(hdfsIs.getPos()).isEqualTo(7L);
    }
  }

  public void testSetOffset() throws IOException {
    try (val hdfsIs = new HdfsDmoInputStream(getTestFile(), null)) {
      val buf = new byte[3];
      assertThat(hdfsIs.read(buf)).isEqualTo(3);
      assertThat(new String(buf)).isEqualTo("ABC");
      assertThat(hdfsIs.getPos()).isEqualTo(3L);

      hdfsIs.seek(5L);
      assertThat(hdfsIs.read(buf)).isEqualTo(3);
      assertThat(new String(buf)).isEqualTo("FGH");
      assertThat(hdfsIs.getPos()).isEqualTo(8L);
    }
  }

  public void testSeekPassEndOfFile() throws IOException {
    try (val hdfsIs = new HdfsDmoInputStream(getTestFile(), null)) {
      assertThatExceptionOfType(IOException.class)
          .isThrownBy(() -> hdfsIs.seek(-1000));
    }
  }

  public void testSeekSamePosition() throws IOException {
    try (val hdfsIs = new HdfsDmoInputStream(getTestFile(), null)) {
      val buf = new byte[3];
      assertThat(hdfsIs.read(buf)).isEqualTo(3);
      assertThat(new String(buf)).isEqualTo("ABC");
      assertThat(hdfsIs.getPos()).isEqualTo(3L);

      hdfsIs.seek(3L);
      assertThat(hdfsIs.read(buf)).isEqualTo(3);
      assertThat(new String(buf)).isEqualTo("DEF");
      assertThat(hdfsIs.getPos()).isEqualTo(6L);
    }
  }

  public void testSeekNegative() throws IOException {
    try (val hdfsIs = new HdfsDmoInputStream(getTestFile(), null)) {
      assertThatExceptionOfType(EOFException.class)
          .isThrownBy(() -> hdfsIs.seek(1000));
    }
  }

  public void testSeekBackwards() throws IOException {
    try (val hdfsIs = new HdfsDmoInputStream(getTestFile(), null)) {
      val buf = new byte[3];
      assertThat(hdfsIs.read(buf)).isEqualTo(3);
      assertThat(new String(buf)).isEqualTo("ABC");
      assertThat(hdfsIs.getPos()).isEqualTo(3L);

      hdfsIs.seek(1L);
      assertThat(hdfsIs.read(buf)).isEqualTo(3);
      assertThat(new String(buf)).isEqualTo("BCD");
      assertThat(hdfsIs.getPos()).isEqualTo(4L);
    }
  }

  public void testReadAfterClose() throws IOException {
    try (val hdfsIs = new HdfsDmoInputStream(getTestFile(), null)) {
      assertThat((char) hdfsIs.read()).isEqualTo('A');
      hdfsIs.close();
      assertThat(hdfsIs.getPos()).isEqualTo(0);

      assertThatExceptionOfType(IOException.class).isThrownBy(hdfsIs::read);
    }
  }

  @Test(timeOut = 1000)
  public void testHdfsReadBuffer() throws IOException {
    val size = 1000000;
    val fileInfo = testUtil.genRandomDmoFile("testHdfsReadBuffer", size);
    try (val is = dmoFs.open(new Path(fileInfo.getName()))) {
      val buf = new byte[size];
      for (int i = 0; i < size; i++) {
        buf[i] = (byte) is.read();
      }
      val crc = new CRC32();
      crc.update(buf, 0, size);
      assertThat(crc.getValue()).isEqualTo(fileInfo.getCrc());
    }
  }

  public void testLargeSeek() throws IOException {
    val size = 1000000;
    val fileInfo = testUtil.genRandomDmoFile("testLargeSeek", size);
    try (val is = dmoFs.open(new Path(fileInfo.getName()))) {
      val head = 95;
      assertThat(is.read(new byte[head], 0, head)).isEqualTo(head);
      assertThat(is.getPos()).isEqualTo(head);

      val nearHead = 300;
      is.seek(nearHead);
      assertThat(is.getPos()).isEqualTo(nearHead);

      val nearTail = 999900;
      is.seek(nearTail);
      assertThat(is.getPos()).isEqualTo(nearTail);
    }
  }

  public void testMaxBufferSize() throws IOException {
    val bufSize = 128 * 1024 * 1024;
    try (val hdfsIs = new HdfsDmoInputStream(getTestFile(), null, bufSize)) {
      assertThat(hdfsIs.getBufferSize())
          .isEqualTo(HdfsDmoInputStream.MAX_BUFFER_SIZE);
    }
  }
}

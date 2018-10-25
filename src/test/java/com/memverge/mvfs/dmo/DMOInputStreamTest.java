/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.dmo;

import static com.memverge.mvfs.TestGroups.IT;
import static com.memverge.mvfs.TestGroups.UT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.memverge.mvfs.utils.FileGenerator;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.zip.CRC32;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Slf4j
@Test(groups = {UT, IT})
public class DMOInputStreamTest {

  private static final String numbers = "0123456789";

  private FileGenerator fileGenerator = new FileGenerator();

  @BeforeMethod
  private void beforeMethod() throws IOException {
    val dmoFile = getTestFile();
    try (val osw = new OutputStreamWriter(
        new DMOOutputStream(dmoFile, false, true))) {
      osw.write(numbers);
    }
  }

  @AfterMethod
  private void afterMethod() {
    DMOFile.safeReset();
  }

  private DMOFile getTestFile() throws IOException {
    return new DMOFile("dmoInputStreamTest");
  }

  public void testSkipPositive() throws IOException {
    val dmoFile = getTestFile();
    try (val dmoIs = new DMOInputStream(dmoFile)) {
      assertThat(dmoIs.skip(5)).isEqualTo(5);
      assertThat((char) dmoIs.read()).isEqualTo('5');
      assertThat(dmoIs.skip(2)).isEqualTo(2);
      assertThat((char) dmoIs.read()).isEqualTo('8');
    }
  }

  public void testReadByteReturnsPositiveInt() throws IOException {
    // 0x4B(181) should not be converted to 75
    int value = 181;
    val dmoFile = new DMOFile("testReadByteReturnsPositiveInt");
    try (val os = new DMOOutputStream(dmoFile, false, true)) {
      os.write(value);
    }
    try (val is = new DMOInputStream(dmoFile)) {
      assertThat(is.read()).isEqualTo(value);
    }
  }

  public void testSkipPassEndOfFile() throws IOException {
    val dmoFile = getTestFile();
    try (val dmoIs = new DMOInputStream(dmoFile)) {
      assertThat(dmoIs.skip(100))
          .isEqualTo(dmoFile.attribute().getSize());
    }
  }

  public void testSkipNegative() throws IOException {
    val dmoFile = getTestFile();
    try (val dmoIs = new DMOInputStream(dmoFile)) {
      assertThat(dmoIs.skip(5)).isEqualTo(5);
      assertThat(dmoIs.skip(-2)).isEqualTo(-2);
      assertThat((char) dmoIs.read()).isEqualTo('3');
    }
  }

  public void testSkipNegativeToBeginning() throws IOException {
    val dmoFile = getTestFile();
    try (val dmoIs = new DMOInputStream(dmoFile)) {
      assertThat(dmoIs.skip(5)).isEqualTo(5);
      assertThat(dmoIs.skip(-100)).isEqualTo(-5);
      assertThat((char) dmoIs.read()).isEqualTo('0');
    }
  }

  public void testAvailable() throws IOException {
    val dmoFile = getTestFile();
    try (val dmoIs = new DMOInputStream(dmoFile)) {
      assertThat(dmoIs.available()).isEqualTo(10);
      assertThat(dmoIs.skip(5)).isEqualTo(5);
      assertThat(dmoIs.available()).isEqualTo(5);
    }
  }

  public void testAvailableWithOffsetLimit() throws IOException {
    val dmoFile = getTestFile();
    try (val dmoIs = new DMOInputStream(dmoFile, 8)) {
      assertThat(dmoIs.available()).isEqualTo(8);
      assertThat(dmoIs.skip(5)).isEqualTo(5);
      assertThat(dmoIs.available()).isEqualTo(3);
    }
  }

  public void testReadLargerThanTotal() throws IOException {
    val dmoFile = getTestFile();
    try (val dmoIs = new DMOInputStream(dmoFile)) {
      val buf = new byte[50];
      val actual = dmoIs.read(buf);
      assertThat(actual).isEqualTo(10);
      val got = Arrays.copyOf(buf, actual);
      assertThat(new String(got)).isEqualTo(numbers);
    }
  }

  public void testActualReadSize() throws IOException {
    val dmoId = "actualReadSize";
    val fileInfo = fileGenerator.genRandomDmoFile(dmoId, 20L);
    try (val dmoIs = new DMOInputStream(new DMOFile(dmoId))) {
      val buf = new byte[50];
      val actualRead = dmoIs.read(buf);
      val crcMaker = new CRC32();
      crcMaker.update(buf, 0, actualRead);

      assertThat(actualRead).isEqualTo(20);
      assertThat(crcMaker.getValue()).isEqualTo(fileInfo.getCrc());
    }
  }

  public void testReadChangesOffset() throws IOException {
    try (val dmoIs = new DMOInputStream(getTestFile())) {
      val buf = new byte[3];
      assertThat(dmoIs.read(buf)).isEqualTo(3);
      assertThat(new String(buf)).isEqualTo("012");
      assertThat(dmoIs.read(buf)).isEqualTo(3);
      assertThat(new String(buf)).isEqualTo("345");
    }
  }

  public void testReadToTheOffsetOfBuffer() throws IOException {
    try (val dmoIs = new DMOInputStream(getTestFile())) {
      val buf = "----".getBytes();
      assertThat(dmoIs.read(buf, 1, 2)).isEqualTo(2);
      assertThat(new String(buf)).isEqualTo("-01-");
      assertThat(dmoIs.read(buf, 0, 1)).isEqualTo(1);
      assertThat(new String(buf)).isEqualTo("201-");
    }
  }

  public void testSeek() throws IOException {
    try (val dmoIs = new DMOInputStream(getTestFile())) {
      val buf = new byte[3];
      assertThat(dmoIs.seek(5)).isEqualTo(5);
      assertThat(dmoIs.read(buf)).isEqualTo(3);
      assertThat(new String(buf)).isEqualTo("567");
    }
  }

  public void testSingleByteRead() throws IOException {
    try (val dmoIs = new DMOInputStream(getTestFile())) {
      val sb = new StringBuilder();
      int ch = 1;
      while (ch > 0) {
        ch = dmoIs.read();
        if (ch > 0) {
          sb.append((char) ch);
        }
      }
      val str = sb.toString();
      assertThat(str).isEqualTo("0123456789");
    }
  }

  public void testEndOffsetInvalidValue() throws IOException {
    try (val dmoIs = new DMOInputStream(getTestFile(), -2)) {
      assertThat(dmoIs.available()).isEqualTo(10);
    }
  }

  public void testEndOffsetAffectsAvailable() throws IOException {
    try (val dmoIs = new DMOInputStream(getTestFile(), 5)) {
      dmoIs.seek(1);
      assertThat(dmoIs.available()).isEqualTo(4);
    }
  }

  public void testEndOffsetLECurrentOffset() throws IOException {
    try (val dmoIs = new DMOInputStream(getTestFile(), 0)) {
      assertThat(dmoIs.available()).isEqualTo(0);
    }

    try (val dmoIs = new DMOInputStream(getTestFile(), 3)) {
      dmoIs.seek(2);
      assertThat(dmoIs.available()).isEqualTo(1);
    }
  }

  public void testSkipBeyondEndOffset() throws IOException {
    try (val dmoIs = new DMOInputStream(getTestFile(), 3)) {
      assertThat(dmoIs.skip(5)).isEqualTo(3);
      assertThat(dmoIs.available()).isEqualTo(0);
    }
  }

  public void testReadWithEndOffset() throws IOException {
    try (val dmoIs = new DMOInputStream(getTestFile(), 6)) {
      assertThat(dmoIs.skip(3)).isEqualTo(3);
      val buf = new byte[2];
      assertThat(dmoIs.read(buf)).isEqualTo(2);
      assertThat(new String(buf)).isEqualTo("34");
      assertThat(dmoIs.available()).isEqualTo(1);

      assertThat(dmoIs.read(buf)).isEqualTo(1);
      assertThat(new String(buf)).startsWith("5");
      assertThat(dmoIs.available()).isEqualTo(0);

      assertThat(dmoIs.read(buf)).isEqualTo(-1);
    }
  }

  public void testReadReachesEndOffset() throws IOException {
    try (val dmoIs = new DMOInputStream(getTestFile(), 6)) {
      assertThat(dmoIs.skip(6)).isEqualTo(6);
      val buf = new byte[2];
      assertThat(dmoIs.read(buf)).isEqualTo(-1);
      assertThat(dmoIs.available()).isEqualTo(0);
    }
  }

  public void testReadEmptySegment() throws IOException {
    try (val dmoIs = new DMOInputStream(getTestFile(), 6)) {
      assertThat(dmoIs.seek(6)).isEqualTo(6);
      val buf = new byte[2];
      assertThat(dmoIs.read(buf)).isEqualTo(-1);
      assertThat(dmoIs.available()).isEqualTo(0);
    }
  }

  public void testSeekBeyondEndOffset() throws IOException {
    try (val dmoIs = new DMOInputStream(getTestFile(), 3)) {
      assertThatExceptionOfType(EOFException.class)
          .isThrownBy(() -> dmoIs.seek(5));
    }
  }

  public void testSeekEOF() throws IOException {
    try (val dmoIs = new DMOInputStream(getTestFile())) {
      assertThatExceptionOfType(EOFException.class)
          .isThrownBy(() -> dmoIs.seek(999));
    }
  }

  public void testMultiThreadRead() throws IOException, InterruptedException {
    val threadCount = 50;
    val repeats = 20000;
    try (val os =
        new BufferedOutputStream(
            new DMOOutputStream(getTestFile(), false, true))) {
      for (int i = 0; i < threadCount; i++) {
        for (int j = 0; j < repeats; j++) {
          os.write(i);
        }
      }
    }

    val threadPool = Executors.newFixedThreadPool(threadCount);
    int taskCount = threadCount * 5;
    val lock = new CountDownLatch(taskCount);
    for (int i = 0; i < taskCount; i++) {
      val j = i % threadCount;
      threadPool.submit(() -> {
        try {
          val asserts = readBlock(j, repeats);
          log.debug("Read block {} success with {} asserts.", j, asserts);
          assertThat(asserts).isEqualTo(repeats);
        } catch (IOException e) {
          log.error("Read block {} failed.", j, e);
        } finally {
          lock.countDown();
        }
      });
    }
    lock.await();
    threadPool.shutdownNow();
  }

  private int readBlock(int i, int repeats) throws IOException {
    val offset = i * repeats;
    int asserts = 0;
    try (val is = getObjectInputStream(getTestFile(), offset)) {
      for (int j = 0; j < repeats; j++) {
        assertThat(is.read()).isEqualTo(i);
        asserts += 1;
      }
    }
    return asserts;
  }

  private InputStream getObjectInputStream(DMOFile file, int offset) {
    val dmoIs = new DMOInputStream(file);
    try {
      dmoIs.seek(offset);
    } catch (IOException ex) {
      log.error("Failed to update offset for {}, offset {}, size {}.",
          file.getDmoId(), offset, file.getSize());
    }

    return new BufferedInputStream(dmoIs);
  }
}

/*
 * Copyright (c) 2018 MemVerge Inc.
 */

package com.memverge.mvfs.sanity;

import static com.memverge.mvfs.TestGroups.IT;
import static com.memverge.mvfs.TestGroups.UT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.internal.collections.Ints.asList;

import com.memverge.mvfs.dmo.DMOFile;
import com.memverge.mvfs.dmo.DMOInputStream;
import com.memverge.mvfs.utils.FileGenerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = {UT, IT})
public class DMODataVerification {

  private final FileGenerator fileGenerator = new FileGenerator();

  @BeforeMethod
  private void beforeMethod() {
    afterMethod();
  }

  @AfterMethod
  private void afterMethod() {
    DMOFile.safeReset();
  }

  @DataProvider(name = "dmoFileArgProvider")
  public Object[] dmoFileArgProvider() {
    val paramList = new ArrayList<DmoFileArg>();
    // small files
    paramList.addAll(getDmoFileArgs(
        asList(
            2 * 1024,
            4 * 1024,
            16 * 1024,
            256 * 1024,
            64 * 1024 * 1024),
        asList(
            1,
            10,
            100,
            10000)));
    // large files
    paramList.addAll(getDmoFileArgs(
        asList(
            2 * 1024 * 1024,
            16 * 1024 * 1024
        ),
        asList(
            48 * 1000 * 1000,
            64 * 1000 * 1000
        )));
    return paramList.toArray();
  }

  private Collection<DmoFileArg> getDmoFileArgs(
      Collection<Integer> chunkSizes, Collection<Integer> fileSizes) {
    return chunkSizes.stream()
        .flatMap(chunkSize -> fileSizes.stream()
            .map(fileSize -> new DmoFileArg(chunkSize, fileSize)))
        .collect(Collectors.toSet());
  }

  @Value
  static class DmoFileArg {

    private int chunkSize;
    private int fileSize;

    public String toString() {
      return "chunkSize=" + chunkSize + ", fileSize=" + fileSize;
    }
  }

  @Test(dataProvider = "dmoFileArgProvider")
  public void testDmoWriteReadFileOfSize(DmoFileArg arg)
      throws IOException {
    val fileSize = arg.getFileSize();
    val chunkSize = arg.getChunkSize();
    val name = String.format("dmoReadWrite_c%d_f%d", chunkSize, fileSize);
    verifyRandomFile(name, chunkSize, fileSize);
  }

  private void verifyRandomFile(String name, int chunkSize, int fileSize) throws IOException {
    val fileInfo = fileGenerator.genRandomDmoFile(name, chunkSize, fileSize);
    val dmoFile = new DMOFile(fileInfo.getName());
    try (val dmoIs = new DMOInputStream(dmoFile)) {
      val dmoSize = (int) dmoFile.attribute().getSize();
      val buf = new byte[dmoSize];
      val crc = new CRC32();
      val bytesRead = dmoIs.read(buf, 0, dmoSize);
      crc.update(buf, 0, dmoSize);
      assertThat(bytesRead).isEqualTo(dmoSize);
      assertThat(crc.getValue()).isEqualTo(fileInfo.getCrc());
    }
  }

  public void testMultiThread() throws InterruptedException {
    val threadCount = 40;
    val executor = Executors.newFixedThreadPool(threadCount);
    val chunkSize = 2 * 1024 * 1024;
    val fileSize = 10 * 1000 * 1000;
    val lock = new CountDownLatch(threadCount);
    for (int i = 0; i < threadCount; i++) {
      val name = String.format("multiThreadTestFile-%d", i);
      executor.submit(() -> {
        try {
          verifyRandomFile(name, chunkSize, fileSize);
        } catch (Exception e) {
          log.error("verify random file raise exception.", e);
        } finally {
          lock.countDown();
        }
        return null;
      });
    }
    lock.await();
  }
}

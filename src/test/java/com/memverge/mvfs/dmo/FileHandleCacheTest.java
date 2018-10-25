/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.dmo;

import static com.memverge.mvfs.TestGroups.IT;
import static com.memverge.mvfs.TestGroups.UT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.memverge.mvfs.connection.MVFSConnectionMgr;
import com.memverge.mvfs.error.MVFSException;
import com.memverge.mvfs.connection.FdManager;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = {UT, IT})
public class FileHandleCacheTest {

  private static final String numbers = "0123456789";

  private FileHandleCache cache = FileHandleCache.getInstance();
  private FdManager manager = FdManager.getInstance();

  @BeforeMethod
  private void beforeMethod() {
    cache.setCacheSize(1000);
    afterMethod();
  }

  @AfterMethod
  private void afterMethod() {
    cache.reset();
    DMOFile.safeReset();
  }

  private DMOFile getTestFile() throws IOException {
    return new DMOFile("/folder/inputFdCache");
  }

  public void testCacheHit() throws IOException {
    int fd;
    try (val is = getTestIs()) {
      assertThat(is.read()).isEqualTo('0');
      fd = is.getFd();
    }
    assertThat(cache.size()).isEqualTo(1);
    // do it again should hit cache
    try (val is = getTestIs()) {
      assertThat(is.read()).isEqualTo('0');
      assertThat(is.getFd()).isEqualTo(fd);
    }
    assertThat(cache.size()).isEqualTo(1);
  }

  public void testInvalidateCache() throws IOException {
    int fd;
    try (val is = getTestIs()) {
      assertThat(is.read()).isEqualTo('0');
      fd = is.getFd();
    }
    cache.invalidate(getTestFile());
    assertThat(cache.size()).isEqualTo(0);
    // do it again should hit cache
    try (val is = getTestIs()) {
      assertThat(is.read()).isEqualTo('0');
      assertThat(is.getFd()).isNotEqualTo(fd);
    }
    assertThat(cache.size()).isEqualTo(1);
  }

  public void testInvalidateSocket() throws IOException {
    readTestFile();
    verifyCacheSize(1);
    cache.invalidate();
    verifyCacheSize(0);
  }

  public void testReset() throws IOException {
    readTestFile();
    verifyCacheSize(1);
    cache.reset();
    verifyCacheSize(0);
  }

  public void testRename() throws IOException {
    readTestFile();
    verifyCacheSize(1);
    getTestFile().rename("newName");
    verifyCacheSize(0);
  }

  public void testRenameWhileReading() throws IOException {
    try (val is = getTestIs()) {
      assertThat(is.read()).isEqualTo('0');
      verifyCacheSize(1);

      getTestFile().rename("newName");
      verifyCacheSize(0);

      assertThatExceptionOfType(MVFSException.class).isThrownBy(is::read);
    }
  }

  @Test(enabled = false)
  public void testRenameParentFolder() throws IOException {
    // TODO: this will fail in current mock implementation
    readTestFile();
    verifyCacheSize(1);
    new DMOFile("/folder").rename("/newFolder/");
    verifyCacheSize(0);
  }

  public void testDelete() throws IOException {
    readTestFile();
    verifyCacheSize(1);
    getTestFile().delete();
    verifyCacheSize(0);
  }

  public void testDeleteWhileReading() throws IOException {
    try (val is = getTestIs()) {
      assertThat(is.read()).isEqualTo('0');
      verifyCacheSize(1);

      getTestFile().delete();
      verifyCacheSize(0);

      assertThatExceptionOfType(MVFSException.class).isThrownBy(is::read);
    }
  }

  @Test(enabled = false)
  public void testDeleteParentFolder() throws IOException {
    // TODO: this will fail because file is open
    readTestFile();
    verifyCacheSize(1);
    new DMOFile("/folder").delete();
    verifyCacheSize(0);
  }

  public void testGetSizeFileNotFound() {
    assertThatExceptionOfType(MVFSException.class)
        .isThrownBy(() -> cache.getSize(new DMOFile("/notFound")));
  }

  public void testGetSizeCached() throws IOException {
    try (val is = getTestIs()) {
      verifyCacheSize(0);
      assertThat(is.getSize()).isEqualTo(10);
      verifyCacheSize(1);
      assertThat(is.getSize()).isEqualTo(10);
      verifyCacheSize(1);
    }
  }

  public void testCacheSizeLargerThanCores() {
    val cores = Runtime.getRuntime().availableProcessors();
    cache.setCacheSize(1);
    createTmpFiles(500);
    assertThat(cache.size()).isEqualTo(cores);
  }

  public void testSetCacheSize() {
    createTmpFiles(300);
    assertThat(cache.size()).isEqualTo(300);

    cache.setCacheSize(200);
    assertThat(cache.size()).isEqualTo(0);

    createTmpFiles(300);
    assertThat(cache.size()).isEqualTo(200);

    cache.setCacheSize(220);
    createTmpFiles(300);
    assertThat(cache.size()).isEqualTo(220);
  }

  private void createTmpFiles(int count) {
    IntStream.range(0, count).parallel().forEach(i -> {
      try {
        val tmpFile = DMOTmpFile.make();
        writeTestFile(tmpFile);
        try (val is = new DMOInputStream(tmpFile)) {
          assertThat(is.read()).isEqualTo('0');
        }
      } catch (IOException e) {
        log.error("failed to create tmp test file.", e);
      }
    });
  }

  private DMOInputStream getTestIs() throws IOException {
    val dmoFile = getTestFile();
    writeTestFile(dmoFile);
    return new DMOInputStream(dmoFile);
  }

  private void writeTestFile(DMOFile dmoFile) throws IOException {
    try (val osw = new OutputStreamWriter(
        new DMOOutputStream(dmoFile, false, true))) {
      osw.write(numbers);
    }
  }

  private void verifyCacheSize(int expected) {
    assertThat(cache.size()).isEqualTo(expected);
    if (MVFSConnectionMgr.isUsingMock() || MVFSConnectionMgr.isWindows()) {
      assertThat(manager.openFdSize()).isEqualTo(expected);
    }
  }

  private void readTestFile() throws IOException {
    try (val is = getTestIs()) {
      assertThat(is.read()).isEqualTo('0');
    }
  }
}

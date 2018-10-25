/*
 * Copyright (c) 2018 MemVerge Inc.
 */

package com.memverge.mvfs.utils;

import static com.memverge.mvfs.TestGroups.UT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.testng.SkipException;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = UT)
public class NativeLoaderTest {

  private boolean isWindows() {
    return System.getProperty("os.name").toLowerCase().contains("windows");
  }

  public void testSaveLibFromRsc() {
    if (isWindows()) {
      throw new SkipException("only valid in non-windows platform.");
    }
    val loader = new NativeLoader("mvfs_jni");
    assertThat(loader.saveLibFromRsc()).contains(".tmp.so");
  }

  public void testSaveLibFromRscNotFound() {
    val loader = new NativeLoader("NA");
    assertThat(loader.saveLibFromRsc()).isNull();
  }
}

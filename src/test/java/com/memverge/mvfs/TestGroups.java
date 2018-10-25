/*
 * Copyright (c) 2018 MemVerge Inc.
 */

package com.memverge.mvfs;

import com.memverge.mvfs.connection.MVFSConnectionMgr;
import com.memverge.mvfs.connection.MockMVFSConnectionFactory;
import com.memverge.mvfs.utils.TmpFileFolder;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterGroups;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;


@Test
@Slf4j
public class TestGroups {

  public static final String UT = "UnitTest";
  public static final String IT = "IntegrationTest";

  @BeforeGroups(UT)
  public void configUT() {
    MVFSConnectionMgr.setConnectionFactory(new MockMVFSConnectionFactory());
  }

  @AfterGroups(UT)
  public void cleanupUT() {
    // remove this line if you want to keep the test resource files.
    TmpFileFolder.cleanup();
  }

  @BeforeGroups(IT)
  public void configIT() {
  }
}

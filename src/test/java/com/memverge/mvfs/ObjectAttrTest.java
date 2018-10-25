/*
 * Copyright (c) 2018 MemVerge Inc.
 */

package com.memverge.mvfs;

import static com.memverge.mvfs.TestGroups.UT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.memverge.mvfs.dmo.ObjectAttr;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = UT)
public class ObjectAttrTest {

  private ObjectAttr attr;

  @BeforeMethod
  private void beforeMethod() {
    attr = new ObjectAttr(
        0,
        262144,
        12345,
        2,
        true,
        1527150211,
        1527150243,
        1527150211
    );
  }

  public void testGetCreationTime() {
    assertThat(attr.getCreationTime()).contains("2018");
  }
}
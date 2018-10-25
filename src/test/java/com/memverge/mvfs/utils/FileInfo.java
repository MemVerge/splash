/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package com.memverge.mvfs.utils;

import lombok.Value;

@Value
public class FileInfo {

  private String name;
  private long crc;
  private long size;
}

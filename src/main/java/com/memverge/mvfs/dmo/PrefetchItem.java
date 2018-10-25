/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.dmo;

import lombok.Value;

@Value
public class PrefetchItem {
  private final String path;
  private final long offset;
  private final long size;
}

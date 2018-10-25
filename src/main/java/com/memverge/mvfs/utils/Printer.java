/*
 * Copyright (c) 2018 MemVerge Inc.
 */
package com.memverge.mvfs.utils;

import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class Printer {

  private static Printer instance = new Printer();

  public static Printer get() {
    return instance;
  }

  public void println(String format, Object... args) {
    val output = String.format(format, args);
    System.out.println(output);
    log.info(output);
  }

  public void printRaw(String raw) {
    System.out.println(raw);
    log.info(raw);
  }
}

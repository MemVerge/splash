/*
 * Copyright (c) 2018  MemVerge Inc.
 */


package com.memverge.mvfs.connection;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;
import org.apache.commons.lang3.StringUtils;


@Data
@AllArgsConstructor
public class MVFSConnectOptions {

  private static String _defaultSocket = initDefaultSocket();
  private static String _defaultLogfile = initDefaultLogfile();

  private static String initDefaultSocket() {
    val envSocket = System.getenv("JMVFS_SOCKET");
    return StringUtils.isEmpty(envSocket) ? "0" : envSocket;
  }

  public static String defaultSocket() {
    return _defaultSocket;
  }

  private static String initDefaultLogfile() {
    val envLogfile = System.getenv("JMVFS_LOGFILE");
    return StringUtils.isEmpty(envLogfile) ? "" : envLogfile;
  }

  public static String defaultLogfile() {
    return _defaultLogfile;
  }

  public MVFSConnectOptions() {
    this(defaultSocket(), defaultLogfile());
  }

  public MVFSConnectOptions(String socket) {
    this(socket, false, false, false, 0, defaultLogfile());
  }

  public MVFSConnectOptions(String socket, String logfile) {
    this(socket, false, false, false, 0, logfile);
  }

  private String socket;
  private boolean warmUp;
  private boolean recordMetrics;
  private boolean enableCache;
  private int prefetchSize;
  private String logfile;
}

/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package com.memverge.mvfs.connection;

public class NativeMVFSConnectionFactory implements MVFSConnectionFactory {

  @Override
  public MVFSConnector build(MVFSConnectOptions options) {
    return new NativeMVFSConnector(options);
  }
}

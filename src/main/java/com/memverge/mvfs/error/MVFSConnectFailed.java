/*
 * Copyright (c) 2018 MemVerge Inc.
 */
package com.memverge.mvfs.error;

public class MVFSConnectFailed extends MVFSException {

  public MVFSConnectFailed(String socket, int rc) {
    super(String.format("Failed to connect to MVFS at '%s'.  error code: %d",
        socket, rc));
  }
}

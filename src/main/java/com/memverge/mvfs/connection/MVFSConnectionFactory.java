/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package com.memverge.mvfs.connection;

public interface MVFSConnectionFactory {

  MVFSConnector build(MVFSConnectOptions options);
}

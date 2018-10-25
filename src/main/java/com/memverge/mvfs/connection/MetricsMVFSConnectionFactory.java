/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.connection;

public class MetricsMVFSConnectionFactory implements MVFSConnectionFactory {

  private MVFSConnectionFactory innerFactory;

  public MetricsMVFSConnectionFactory(MVFSConnectionFactory factory) {
    innerFactory = factory;
  }

  @Override
  public MVFSConnector build(MVFSConnectOptions options) {
    return new MetricsMVFSConnector(innerFactory.build(options));
  }
}

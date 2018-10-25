/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package com.memverge.mvfs.connection;

import com.memverge.mvfs.error.MVFSException;
import com.memverge.mvfs.metrics.DMOMetricsMgr;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class MVFSConnectionMgr implements Closeable {

  private static MVFSConnectionMgr instance = new MVFSConnectionMgr();

  private final Map<String, MVFSConnector> connectorMap;

  private final Map<String, MVFSConnectOptions> connectOptionsMap;

  private MVFSConnectionFactory connectionFactory;

  public static void setConnectionFactory(MVFSConnectionFactory factory) {
    log.info("set connection factory to: {}", factory.getClass());
    instance.connectionFactory = instance.wrapMetrics(factory);
    cleanup();
  }

  public static boolean isWindows() {
    val os = System.getProperty("os.name");
    return os != null && os.toLowerCase().contains("windows");
  }

  public static boolean isUsingMock() {
    return StringUtils.isNotEmpty(System.getenv("JMVFS_MOCK"));
  }

  private MVFSConnectionMgr() {
    connectorMap = new ConcurrentHashMap<>();
    connectOptionsMap = new ConcurrentHashMap<>();

    connectionFactory = initConnectionFactory();
  }

  private MVFSConnectionFactory initConnectionFactory() {
    MVFSConnectionFactory ret;
    if (isUsingMock() || isWindows()) {
      log.info("use jmvfs mock instead of native connector.");
      ret = new MockMVFSConnectionFactory();
    } else {
      ret = new NativeMVFSConnectionFactory();
    }
    ret = wrapMetrics(ret);
    return ret;
  }

  private MVFSConnectionFactory wrapMetrics(MVFSConnectionFactory ret) {
    if (log.isDebugEnabled() || DMOMetricsMgr.getInstance().isEnabled()) {
      ret = new MetricsMVFSConnectionFactory(ret);
    }
    return ret;
  }

  public static MVFSConnector get(String socketPath) throws MVFSException {
    return instance.getConnector(socketPath);
  }

  public static void setConnectOptions(MVFSConnectOptions options) {
    instance.setConnectOptions(options.getSocket(), options);
  }

  public static void cleanup() {
    instance.close();
  }

  private MVFSConnector getConnector(String socket) throws MVFSException {
    if (socket == null) {
      socket = "";
    }
    val connector = connectorMap.computeIfAbsent(socket, this::addConnector);
    if (!connector.isConnected()) {
      connector.connect();
    }
    return connector;
  }

  synchronized private void setConnectOptions(String socketPath, MVFSConnectOptions options) {
    connectOptionsMap.put(socketPath, options);
  }

  private MVFSConnector addConnector(String socketPath) {
    log.info("add new connector for socket: {}", socketPath);
    val options = connectOptionsMap.computeIfAbsent(socketPath,
        MVFSConnectOptions::new);
    return connectionFactory.build(options);
  }

  @Override
  public void close() {
    for (val socket : connectorMap.keySet()) {
      try {
        connectorMap.remove(socket).close();
      } catch (IOException ex) {
        log.error("Failed to close socket: {}.", socket, ex);
      }
    }
  }
}

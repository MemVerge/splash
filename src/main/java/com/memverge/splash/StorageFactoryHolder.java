/*
 * Copyright (c) 2018 MemVerge Inc.
 */
package com.memverge.splash;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.SplashOpts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageFactoryHolder {

  private static final Logger logger = LoggerFactory
      .getLogger(StorageFactoryHolder.class);

  private static final StorageFactoryHolder INSTANCE = new StorageFactoryHolder();

  public static StorageFactory getFactory() {
    return INSTANCE.getRealFactory();
  }

  private SparkConf conf;

  public static void setSparkConf(SparkConf conf) {
    INSTANCE.conf = conf;
  }

  private SparkConf getSparkConf() {
    if (conf == null) {
      SparkEnv sparkEnv = SparkEnv.get();
      if (sparkEnv != null) {
        SparkConf sparkConf = sparkEnv.conf();
        if (sparkConf != null) {
          conf = sparkConf;
        }
      }
    }
    return conf;
  }

  private StorageFactory factory = null;

  private StorageFactory getRealFactory() {
    if (factory == null) {
      synchronized (this) {
        if (factory == null) {
          SparkConf sparkConf = getSparkConf();
          String clzName;
          if (sparkConf != null) {
            clzName = sparkConf.get(SplashOpts.storageFactoryName());
          } else {
            clzName = SplashOpts.storageFactoryName().defaultValueString();
          }
          factory = initFactory(clzName);
        }
      }
    }
    return factory;
  }

  public static void onApplicationStart() {
    getListeners().forEach(ShuffleListener::onApplicationStart);
  }

  private static Collection<ShuffleListener> getListeners() {
    Collection<ShuffleListener> ret = INSTANCE.getRealFactory().getListeners();
    if (ret == null) {
      ret = Collections.emptyList();
    }
    return ret;
  }

  public static void onApplicationEnd() {
    getListeners().forEach(ShuffleListener::onApplicationEnd);
  }

  private StorageFactory initFactory(String clzName) {
    StorageFactory ret;
    try {
      logger.info("Create storage factory from class: {}", clzName);
      Class<?> clazz = Class.forName(clzName);
      Constructor<?> constructor = clazz.getConstructor();
      ret = (StorageFactory) constructor.newInstance();
    } catch (ReflectiveOperationException e) {
      final String msg = String.format(
          "cannot use %s as a storage factory.",
          clzName);
      logger.error(msg, e);
      throw new IllegalArgumentException(msg, e);
    }
    return ret;
  }
}

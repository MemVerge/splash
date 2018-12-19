/*
 * Copyright (C) 2018 MemVerge Corp
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.memverge.splash;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import org.apache.spark.SparkConf;
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

  private SparkConf conf = null;

  public static void setSparkConf(SparkConf sparkConf) {
    INSTANCE.conf = sparkConf;
    logger.info("initialize StorageFactoryHolder with {}", sparkConf);
    INSTANCE.factory = null;
    logger.info("SparkConf has been updated.  reset StorageFactory to null.");
  }

  private StorageFactory factory = null;

  private StorageFactory getRealFactory() {
    if (factory == null) {
      synchronized (this) {
        if (factory == null) {
          String clzName;
          if (conf != null) {
            clzName = conf.get(SplashOpts.storageFactoryName());
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
      ret.setConf(conf);
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

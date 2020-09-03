package com.memverge.splash.dfs;

import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.SplashOpts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

class FileContextManager {
  private static final Logger log = LoggerFactory.getLogger(FileContextManager.class);
  private static final ConcurrentHashMap<Configuration, FileContext> contextMap = new ConcurrentHashMap<>();
  private static final Configuration hadoopConf = new Configuration();

  static {
    val sparkConf = Optional
      .ofNullable(SparkEnv.get())
      .map(SparkEnv::conf)
      .orElse(new SparkConf(false));
    val splashFolder = sparkConf
      .get(SplashOpts.localSplashFolder().key(), hadoopConf.get(FS_DEFAULT_NAME_KEY, FS_DEFAULT_NAME_DEFAULT));
    hadoopConf.set(FS_DEFAULT_NAME_KEY, splashFolder);
  }

  static FileContext getOrCreate(Configuration conf) {
    log.info("Creating file context for {}", conf.get(FS_DEFAULT_NAME_KEY));
    return contextMap.computeIfAbsent(conf, k -> {
      try {
        return FileContext.getFileContext(conf);
      } catch (UnsupportedFileSystemException e) {
        throw new RuntimeException(e);
      }
    });
  }

  static FileContext getOrCreate() {
    return getOrCreate(hadoopConf);
  }

  static Configuration getHadoopConf() {
    return hadoopConf;
  }
}

/*
 * Copyright (C) 2018 MemVerge Inc.
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
package com.memverge.splash.dfs;

import com.google.common.io.Closeables;
import com.memverge.splash.ShuffleFile;
import lombok.val;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.SplashOpts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.EnumSet;
import java.util.Optional;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

public class DistributedFSShuffleFile implements ShuffleFile {

  private static final Logger log = LoggerFactory.getLogger(DistributedFSShuffleFile.class);
  private static final String splashFolder;
  private static final FileContext fileContext;

  static {
    val sparkConf = Optional
      .ofNullable(SparkEnv.get())
      .map(SparkEnv::conf)
      .orElse(new SparkConf(false));
    val hadoopConf = FileContextManager.getHadoopConf();
    splashFolder = sparkConf
      .get(SplashOpts.localSplashFolder().key(), hadoopConf.get(FS_DEFAULT_NAME_KEY, FS_DEFAULT_NAME_DEFAULT));
    hadoopConf.set(FS_DEFAULT_NAME_KEY, splashFolder);
    fileContext = FileContextManager.getOrCreate();
    log.info("Using file system {} for shuffle", splashFolder);
  }

  protected Path underlyingFilePath;

  DistributedFSShuffleFile(String path) {
    this.underlyingFilePath = new Path(splashFolder, path);
  }

  @Override
  public long getSize() {
    long len = 0;
    try {
      len = fileContext.getFileStatus(underlyingFilePath).getLen();
    } catch (IOException ioe) {
      val msg = String.format("get size failed for %s.", getPath());
      throw new IllegalArgumentException(msg, ioe);
    }
    return len;
  }

  @Override
  public boolean delete() throws IOException {
    return fileContext.delete(underlyingFilePath, true);
  }

  @Override
  public boolean exists() throws IOException {
    return fileContext.util().exists(underlyingFilePath);
  }

  @Override
  public String getPath() {
    return underlyingFilePath.toString();
  }

  @Override
  public InputStream makeInputStream() {
    InputStream distFileStream;
    try {
      distFileStream = fileContext.open(underlyingFilePath);
      log.debug("Create input stream for {}.", getPath());
    } catch (IOException e) {
      val msg = String.format("Unable to create input stream for %s.", getPath());
      throw new IllegalArgumentException(msg, e);
    }
    return distFileStream;
  }

  void rename(String tgtId) throws IOException {
    val destPath = new Path(URI.create(tgtId));
    val createFlags = EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
    Closeables.close(fileContext.create(destPath, createFlags, Options.CreateOpts.createParent()), false);
    fileContext.rename(underlyingFilePath, destPath, Options.Rename.OVERWRITE);
  }

  protected FileContext getFileContext() {
    return fileContext;
  }
}

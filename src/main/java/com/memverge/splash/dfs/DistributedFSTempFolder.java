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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.SplashOpts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class DistributedFSTempFolder {
  private static final Logger log = LoggerFactory.getLogger(DistributedFSTempFolder.class);
  private static final DistributedFSTempFolder INSTANCE = new DistributedFSTempFolder();
  private static final FileContext fileContext = FileContextManager.getOrCreate();

  public static DistributedFSTempFolder getInstance() {
    return INSTANCE;
  }

  private DistributedFSTempFolder() {
  }

  private String getSplashPath() {
    String folder = null;
    SparkEnv env = SparkEnv.get();
    if (env != null) {
      SparkConf conf = env.conf();
      if (conf != null) {
        folder = conf.get(SplashOpts.localSplashFolder());
      }
    }
    if (folder == null) {
      String tmpFolder = System.getProperty("java.io.tmpdir");
      folder = String.format("%s/%s/splash", tmpFolder, getUser());
    }
    return folder;
  }

  private String getUser() {
    String user = System.getProperty("user.name");
    return StringUtils.isEmpty(user) ? "unknown" : user;
  }

  public String getTmpPath() {
    Path path = new Path(getSplashPath(), "tmp");
    ensureFolderExists(path);
    return path.toString();
  }

  public String getShufflePath(String appId) {
    Path path = new Path(String.format("%s/%s/shuffle", getSplashPath(), appId));
    ensureFolderExists(path);
    return path.toString();
  }

  public int countShuffleFile(String appId) throws IOException {
    Path file = new Path(getShufflePath(appId));
    FileStatus[] fileStatuses = Optional.ofNullable(fileContext.util().listStatus(file)).orElse(new FileStatus[]{});
    return Long.valueOf(Arrays.stream(fileStatuses).map(FileStatus::getLen).mapToLong(Long::longValue).sum()).intValue();
  }

  public int countTmpFile() throws IOException {
    return Objects.requireNonNull(fileContext.util().listStatus(new Path(getTmpPath()))).length;
  }

  public void reset() {
    String path = getSplashPath();
    try {
      fileContext.delete(new Path(path), true);
    } catch (FileNotFoundException e) {
      log.debug("{} not exists.  do nothing.", path);
    } catch (IOException e) {
      log.error("failed to clean up local splash folder: {}", path, e);
    }
  }

  private void ensureFolderExists(Path path) {
    try {
      Path folder;
      if (fileContext.util().exists(path)) {
        FileStatus[] statuses = fileContext.util().listStatus(path);
        List<FileStatus> file = Arrays.stream(statuses).filter(fs -> fs.getPath().equals(path)).collect(Collectors.toList());
        if (file.isEmpty() || file.get(0).isFile()) {
          folder = path.getParent();
        } else {
          folder = file.get(0).getPath();
        }
      } else {
        folder = path;
      }
      fileContext.mkdir(folder, null, true);
      log.info("Create folder {}", path.getParent());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}

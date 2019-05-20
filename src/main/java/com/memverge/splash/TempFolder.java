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
package com.memverge.splash;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.SplashOpts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TempFolder {

  private static final Logger log = LoggerFactory.getLogger(TempFolder.class);

  private static final TempFolder INSTANCE = new TempFolder();

  public static TempFolder getInstance() {
    return INSTANCE;
  }

  private TempFolder() {
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
      Path path = Paths.get(tmpFolder, getUser());
      folder = Paths.get(path.toString(), "splash").toString();
    }
    return folder;
  }

  private String getUser() {
    String user = System.getProperty("user.name");
    return StringUtils.isEmpty(user) ? "unknown" : user;
  }

  public String getTmpPath() {
    Path path = Paths.get(getSplashPath(), "tmp");
    ensureFolderExists(path);
    return path.toString();
  }

  public String getShufflePath(String appId) {
    Path path = Paths.get(getSplashPath(), appId, "shuffle");
    ensureFolderExists(path);
    return path.toString();
  }

  public int countShuffleFile(String appId) {
    int shuffleFileCount = 0;
    File file = new File(getShufflePath(appId));
    File[] files = file.listFiles();
    if (files != null) {
      for (File child : files) {
        if (child.isFile()) {
          shuffleFileCount += 1;
        } else {
          String[] grandChildren = child.list();
          if (grandChildren != null) {
            shuffleFileCount += grandChildren.length;
          }
        }
      }
    }
    return shuffleFileCount;
  }

  public int countTmpFile() {
    return Objects.requireNonNull(new File(getTmpPath()).list()).length;
  }

  public void reset() {
    String path = getSplashPath();
    try {
      FileUtils.forceDelete(new File(path));
    } catch (FileNotFoundException e) {
      log.debug("{} not exists.  do nothing.", path);
    } catch (IOException e) {
      log.error("failed to clean up local splash folder: {}", path, e);
    }
  }

  private void ensureFolderExists(Path path) {
    File file = path.toFile();
    File folder;
    if (file.isFile()) {
      folder = file.getParentFile();
    } else {
      folder = file;
    }
    if (!folder.exists() && folder.mkdirs()) {
      log.info("Create folder {}", file.getParent());
    }
  }
}

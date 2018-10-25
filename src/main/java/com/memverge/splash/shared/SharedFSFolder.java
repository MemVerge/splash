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
package com.memverge.splash.shared;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SharedFSFolder {

  private static final Logger log = LoggerFactory
      .getLogger(SharedFSFolder.class);

  private static final SharedFSFolder INSTANCE = new SharedFSFolder();

  static SharedFSFolder getInstance() {
    return INSTANCE;
  }

  private SharedFSFolder() {
  }

  private String getSplashPath() {
    String tmpFolder = System.getProperty("java.io.tmpdir");
    Path path = Paths.get(tmpFolder, getUser());
    String folder = "splash";
    return Paths.get(path.toString(), folder).toString();
  }

  private String getUser() {
    String user = System.getProperty("user.name");
    return StringUtils.isEmpty(user) ? "unknown" : user;
  }

  String getTmpPath() {
    Path path = Paths.get(getSplashPath(), "tmp");
    ensureFolderExists(path);
    return path.toString();
  }

  String getShufflePath(String appId) {
    Path path = Paths.get(getSplashPath(), appId, "shuffle");
    ensureFolderExists(path);
    return path.toString();
  }

  int countShuffleFile(String appId) {
    return Objects
        .requireNonNull(new File(getShufflePath(appId)).list()).length;
  }

  int countTmpFile() {
    return Objects.requireNonNull(new File(getTmpPath()).list()).length;
  }

  void reset() {
    String path = getSplashPath();
    try {
      FileUtils.forceDelete(new File(path));
    } catch (FileNotFoundException e) {
      // do nothing
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

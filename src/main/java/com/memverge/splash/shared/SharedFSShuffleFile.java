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

import com.memverge.splash.ShuffleFile;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedFSShuffleFile implements ShuffleFile {

  private static final Logger log = LoggerFactory
      .getLogger(SharedFSShuffleFile.class);

  protected File file;

  SharedFSShuffleFile(String pathname) {
    file = new File(pathname);
  }

  @Override
  public ShuffleFile create() throws IOException {
    boolean created = getFile().createNewFile();
    if (!created) {
      log.warn("file {} already exists.", getId());
    } else {
      log.debug("file {} created.", getId());
    }
    return this;
  }

  @Override
  public long getSize() {
    return file.length();
  }

  @Override
  public boolean delete() {
    return file.delete();
  }

  @Override
  public boolean exists() {
    return file.exists();
  }

  @Override
  public String[] list() {
    return file.list();
  }

  @Override
  public String getId() {
    return file.getAbsolutePath();
  }

  @Override
  public InputStream makeInputStream() {
    InputStream ret;
    try {
      ret = new FileInputStream(file);
      log.debug("create input stream for {}.", getId());
    } catch (FileNotFoundException e) {
      String msg = String.format("Create input stream failed for %s.", getId());
      throw new IllegalArgumentException(msg, e);
    }
    return ret;
  }

  @Override
  public OutputStream makeOutputStream(boolean append, boolean create) {
    if (!exists()) {
      if (create) {
        try {
          create();
        } catch (IOException e) {
          String msg = String.format("Create file %s failed.", getId());
          throw new IllegalArgumentException(msg, e);
        }
      } else {
        String msg = String.format("%s not found.", getId());
        throw new IllegalArgumentException(msg);
      }
    }
    OutputStream ret;
    try {
      ret = new FileOutputStream(file, append);
      log.debug("create output stream for {}.", getId());
    } catch (FileNotFoundException e) {
      String msg = String.format("File %s not found?", getId());
      throw new IllegalArgumentException(msg, e);
    }
    return ret;
  }

  private File getFile() {
    return file;
  }

  void rename(String tgtId) throws IOException {
    File tgtFile = new File(tgtId);
    boolean success = file.renameTo(tgtFile);
    if (success) {
      log.debug("rename {} to {}.", getId(), tgtId);
    } else {
      String msg = String.format("rename file to %s failed.", tgtId);
      throw new IOException(msg);
    }
  }
}

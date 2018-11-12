package com.memverge.splash.local;

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

public class LocalShuffleFile implements ShuffleFile {

  private static final Logger log = LoggerFactory
      .getLogger(LocalShuffleFile.class);

  private File file;

  @Override
  public ShuffleFile create() throws IOException {
    // TODO: Update this if it doesn't suit your needs
    boolean created = getFile().createNewFile();
    if (!created) {
      log.warn("file {} already exists.", file.getAbsolutePath());
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
    // TODO: Update this if it doesn't suit your needs
    return file.getAbsolutePath();
  }

  @Override
  public void reset() {

  }

  @Override
  public InputStream makeInputStream() {
    // TODO: Need to test this
    InputStream ret;
    try {
      ret = new FileInputStream(file);
    } catch (FileNotFoundException e) {
      String msg = String.format("Create input stream failed for %s.",
          file.getAbsolutePath());
      throw new IllegalArgumentException(msg, e);
    }
    return ret;
  }

  @Override
  public OutputStream makeOutputStream(boolean append, boolean create) {
    // TODO: Need to test this
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
    } catch (FileNotFoundException e) {
      String msg = String.format("File %s not found?", getId());
      throw new IllegalArgumentException(msg, e);
    }
    return ret;
  }

  private File getFile() {
    return file;
  }
}

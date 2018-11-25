/*
 * Copyright (c) 2018 MemVerge Inc.
 */
package com.memverge.splash;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface ShuffleFile {
  ShuffleFile create() throws IOException;

  long getSize();

  boolean delete() throws IOException;

  boolean exists() throws IOException;

  String[] list() throws IOException;

  String getId();

  void reset();

  InputStream makeInputStream();

  default OutputStream makeOutputStream() {
   return makeOutputStream(false);
  }

  default OutputStream makeOutputStream(boolean create) {
    return makeOutputStream(false, create);
  }

  OutputStream makeOutputStream(boolean append, boolean create);
}

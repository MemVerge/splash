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

  InputStream makeInputStream();

  default OutputStream makeOutputStream() {
   return makeOutputStream(false);
  }

  default OutputStream makeOutputStream(boolean create) {
    return makeOutputStream(false, create);
  }

  OutputStream makeOutputStream(boolean append, boolean create);
}

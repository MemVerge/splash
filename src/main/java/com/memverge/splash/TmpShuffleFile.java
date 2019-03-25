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

import com.google.common.io.Closeables;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface TmpShuffleFile extends ShuffleFile {

  TmpShuffleFile create() throws IOException;

  void swap(TmpShuffleFile other) throws IOException;

  ShuffleFile getCommitTarget();

  /**
   * Commit the tmp file to the target file. The implementation of this method
   * must be atomic. If the commit target already exists, it should be removed.
   */
  ShuffleFile commit() throws IOException;

  void recall();

  UUID uuid();

  OutputStream makeOutputStream();
}

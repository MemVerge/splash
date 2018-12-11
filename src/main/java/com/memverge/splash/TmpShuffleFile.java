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

  ShuffleFile commit() throws IOException;

  void recall();

  UUID uuid();

  default OutputStream makeOutputStream() {
    return makeOutputStream(false);
  }

  default OutputStream makeOutputStream(boolean create) {
    return makeOutputStream(false, create);
  }

  OutputStream makeOutputStream(boolean append, boolean create);

  default List<Long> merge(Collection<? extends ShuffleFile> srcFiles)
      throws IOException {
    final Logger log = LoggerFactory.getLogger(TmpShuffleFile.class);
    final OutputStream out = makeOutputStream(true);
    final List<Long> lengths = srcFiles.stream().map(file -> {
      Long copied = null;
      try (final InputStream in = file.makeInputStream()) {
        copied = (long) IOUtils.copy(in, out);
      } catch (IOException e) {
        log.error("merge input from {} to {} failed.",
            file.getId(), getId(), e);
      } finally {
        try {
          file.delete();
        } catch (IOException e) {
          log.warn("delete {} failed.", file.getId(), e);
        }
      }
      return copied;
    }).collect(Collectors.toList());
    final boolean threwException = lengths.stream().anyMatch(Objects::isNull);
    Closeables.close(out, threwException);
    return lengths;
  }
}

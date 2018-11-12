/*
 * Copyright (c) 2018 MemVerge Inc.
 */
package com.memverge.splash;

import java.io.IOException;
import java.util.UUID;

public interface TmpShuffleFile extends ShuffleFile {

  void swap(TmpShuffleFile other) throws IOException;

  ShuffleFile getCommitTarget();

  ShuffleFile commit() throws IOException;

  void recall();

  UUID uuid();
}

/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package com.memverge.mvfs.connection;

import com.memverge.mvfs.dmo.ObjectAttr;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
class MockDmoAttr {

  private final String path;
  private final int chunkSize;
  private final int replicas;
  private final int padding;

  public ObjectAttr getObjectAttr(BasicFileAttributes fileAttr) {
    return new ObjectAttr(
        0,
        chunkSize,
        fileAttr.size(),
        replicas,
        !fileAttr.isDirectory(),
        toEpoch(fileAttr.creationTime()),
        toEpoch(fileAttr.lastModifiedTime()),
        toEpoch(fileAttr.lastAccessTime())
    );
  }

  private long toEpoch(FileTime time) {
    return time.toInstant().getEpochSecond();
  }
}

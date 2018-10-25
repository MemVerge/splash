/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package com.memverge.mvfs.dmo;

import java.util.Date;
import lombok.AccessLevel;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.Wither;
import lombok.val;

@Value
public class ObjectAttr {

  @Wither(AccessLevel.PUBLIC)
  @NonFinal
  private final int rc;
  private final long chunkSize;
  private final long size;
  private final int replicaCount;
  private final boolean isFile;
  // time of creation
  private final long bTime;
  // time of last modification
  private final long mTime;
  // time of last access
  private final long aTime;

  public boolean isValid() {
    return rc >= 0 && getSize() >= 0;
  }

  public String getCreationTime() {
    return getLocalTimeString(bTime);
  }

  public String getModifiedTime() {
    return getLocalTimeString(mTime);
  }

  public String getAccessTime() {
    return getLocalTimeString(aTime);
  }

  private String getLocalTimeString(long ticks) {
    return new Date(ticks * 1000).toString();
  }

  @Override
  public String toString() {
    val type = isFile() ? "File" : "Directory";
    return label("Chunk Size") + chunkSize + '\n'
        + label("Size") + size + '\n'
        + label("Replica Count") + replicaCount + '\n'
        + label("Type") + type + '\n'
        + label("Create") + getCreationTime() + '\n'
        + label("Modify") + getModifiedTime() + '\n'
        + label("Access") + getAccessTime();
  }

  private String label(String display) {
    return String.format("%-20s", display + ':');
  }

}

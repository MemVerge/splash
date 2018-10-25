/*
 * Copyright (c) 2018 MemVerge Inc.
 */

package com.memverge.mvfs.error;


import com.memverge.mvfs.dmo.DMOFile;
import java.io.IOException;
import java.util.ArrayList;
import lombok.Getter;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

public class MVFSException extends IOException {

  @Getter
  private final Integer rc;

  @Getter
  private final String dmoId;

  public MVFSException(String message) {
    this(message, "", null);
  }

  public MVFSException(String message, DMOFile dmoFile) {
    this(message, dmoFile.getDmoId(), null);
  }

  public MVFSException(String message, DMOFile dmoFile, Integer rc) {
    this(message, dmoFile.getDmoId(), rc);
  }

  public MVFSException(String message, String dmoId, Integer rc) {
    super(getMessage(message, dmoId, rc));
    this.rc = rc;
    this.dmoId = dmoId;
  }

  private static String getMessage(String message, String dmoId, Integer rc) {
    val msgList = new ArrayList<String>();
    msgList.add(message);
    if (!StringUtils.isEmpty(dmoId)) {
      msgList.add(String.format("dmo id: \"%s\"", dmoId));
    }
    if (rc != null) {
      msgList.add(String.format("rc: %d.", rc));
    }
    if (msgList.size() > 1) {
      msgList.set(0, StringUtils.stripEnd(message, "."));
    }
    return StringUtils.join(msgList, ", ").trim();
  }
}

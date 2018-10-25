package com.memverge.mvfs.error;

import com.memverge.mvfs.dmo.DMOFile;

public class MVFSDirNotEmpty extends MVFSException {

  public MVFSDirNotEmpty(String dmoId) {
    super("Directory is not empty.", dmoId, DMOFile.ERROR_DIR_NOT_EMPTY);
  }
}

package com.memverge.splash.local;

import com.memverge.splash.ShuffleFile;
import com.memverge.splash.TmpShuffleFile;
import java.io.IOException;
import java.util.UUID;

public class LocalTmpShuffleFile extends LocalShuffleFile implements TmpShuffleFile {

  @Override
  public void swap(TmpShuffleFile other) throws IOException {
    //TODO:
  }

  @Override
  public ShuffleFile getCommitTarget() {
    //TODO:
    return null;
  }

  @Override
  public ShuffleFile commit() throws IOException {
    //TODO:
    return null;
  }

  @Override
  public void recall() {
    //TODO:
  }

  @Override
  public UUID uuid() {
    //TODO:
    return null;
  }
}

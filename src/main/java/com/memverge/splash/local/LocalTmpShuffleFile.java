package com.memverge.splash.local;

import com.memverge.splash.ShuffleFile;
import com.memverge.splash.TmpShuffleFile;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class LocalTmpShuffleFile extends LocalShuffleFile implements TmpShuffleFile {
  private static final String TMP_FILE_PREFIX = "tmp-";
  private LocalShuffleFile commitTarget = null;

  private UUID uuid = null;
  private String targetId = null;
  private static final Logger log = LoggerFactory
      .getLogger(LocalTmpShuffleFile.class);

  private static final LocalTmpFileFolder tmpFolder = new LocalTmpFileFolder("local-tmp");
  public LocalTmpShuffleFile() {
    super();
  }

  public LocalTmpShuffleFile(String pathname)
      throws IOException {
    super(pathname);
  }

  public static LocalTmpShuffleFile make() throws IOException {
    //TODO:Done
    UUID uuid = UUID.randomUUID();
    String uuidStr = new File(folderPrefix(),
        String.format("%s%s", TMP_FILE_PREFIX, uuid.toString())).toString();
    LocalTmpShuffleFile ret = new LocalTmpShuffleFile(uuidStr);
    ret.create();
    ret.uuid = uuid;
    return ret;
  }

  public static LocalTmpShuffleFile make(ShuffleFile file) throws IOException {
    //TODO:Done
    if (file == null) {
      throw new IOException("Make null File as file");
    }
    LocalTmpShuffleFile ret = make();
    ret.commitTarget = (LocalShuffleFile) file;
    ret.targetId = file.getId();
    return ret;
  }

  @Override
  public void swap(TmpShuffleFile other) throws IOException {
    //TODO:Done
    if (!other.exists()) {
      String message = "Can only swap with a uncommitted tmp file";
      throw new IOException(message);
    }

    LocalTmpShuffleFile localTmpShuffleFileOther = (LocalTmpShuffleFile) other;
    if (localTmpShuffleFileOther == null) {
      throw new IllegalArgumentException("only accept LocalTmpShuffleFile instance");
    }

    forceDelete();
    UUID otherUuid = localTmpShuffleFileOther.uuid;
    String otherLocalId = localTmpShuffleFileOther.localId;
    localTmpShuffleFileOther.uuid = this.uuid;
    localTmpShuffleFileOther.localId = this.localId;
    this.uuid = otherUuid;
    this.localId = otherLocalId;
    File tmp = this.file;
    this.file = localTmpShuffleFileOther.file;
    localTmpShuffleFileOther.file = tmp;
  }

  @Override
  public LocalShuffleFile getCommitTarget() {
    //TODO:Done
    return this.commitTarget;
  }

  @Override
  public ShuffleFile commit() throws IOException {
    if (commitTarget == null) {
      throw new IOException("No commit target.");
    } else if (!exists()) {
      throw new IOException("Tmp file already committed or recalled.");
    }
    if (commitTarget.exists()) {
      log.warn("commit target already exists, remove '{}'.", commitTarget.getLocalId());
      commitTarget.forceDelete();
    }
    log.debug("commit tmp file {} to target file {}.",
        getLocalId(), getCommitTarget().getLocalId());

    rename(this.targetId);
    return commitTarget;
  }

  @Override
  public void recall() {
    //TODO:Done
    try {
      LocalShuffleFile commitTarget = getCommitTarget();
      if (commitTarget != null) {
        log.info("recall tmp file {} of target file {}.",
            getLocalId(), commitTarget.getLocalId());
      } else {
        log.info("recall tmp file {} without target file.", getLocalId());
      }
      delete();
    }catch (IOException e) {
        log.warn("Error while deleting '{}'.  Ignore it due to force.", localId);
    }
  }

  @Override
  public UUID uuid() {
    //TODO:Done
    return this.uuid;
  }

  public static String[] listAll() throws IOException {
    return new LocalShuffleFile(folderPrefix()).list();
  }

  public int getTmpFileCount() throws IOException {
    return this.listAll().length;
  }

  private static String folderPrefix() {
    return new File(tmpFolder.getTmpPath()).toString();
  }
}

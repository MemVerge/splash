package com.memverge.mvfs.dmo;

import com.memverge.mvfs.connection.MVFSConnectOptions;
import com.memverge.mvfs.error.MVFSException;
import java.io.IOException;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class DMOTmpFile extends DMOFile {

  private static final String TMP_FILE_PREFIX = "tmp-";

  private static final String TMP_FOLDER = "tmp";

  private DMOFile commitTarget = null;

  private UUID uuid = null;

  private DMOTmpFile(String socketPath, String dmoId, int chunkSize)
      throws IOException {
    super(socketPath, dmoId, chunkSize);
  }

  public static DMOTmpFile make() throws IOException {
    return make(MVFSConnectOptions.defaultSocket(), DMOFile.CHUNK_SIZE);
  }

  public static DMOTmpFile make(String socketPath, int chunkSize)
      throws IOException {
    val uuid = UUID.randomUUID();
    val uuidStr = folderPrefix() +
        String.format("%s%s", TMP_FILE_PREFIX, uuid.toString());
    val ret = new DMOTmpFile(socketPath, uuidStr, chunkSize);
    ret.uuid = uuid;
    ret.create();
    return ret;
  }

  public static DMOTmpFile make(DMOFile file) throws IOException {
    val ret = make(file.getMvfsConn().getOptions().getSocket(), file.getChunkSize());
    ret.commitTarget = file;
    return ret;
  }

  public String getName() {
    return getDmoId().replaceFirst(folderPrefix(), "");
  }

  public void recall() {
    try {
      val commitTarget = getCommitTarget();
      if (commitTarget != null) {
        log.info("recall tmp file {} of target file {}.",
            getDmoId(), commitTarget.getDmoId());
      } else {
        log.info("recall tmp file {} without target file.", getDmoId());
      }
      delete();
    } catch (IOException ex) {
      log.warn("Error while deleting '{}'.  Ignore it due to force.", dmoId);
    }
  }

  public DMOFile commit() throws IOException {
    if (commitTarget == null) {
      throw new MVFSException("No commit target.", this);
    } else if (!exists()) {
      throw new MVFSException("Tmp file already committed or recalled.", this);
    }
    val tmpDmoId = getDmoId();
    if (commitTarget.exists()) {
      log.warn("commit target already exists, remove '{}'.", commitTarget.getDmoId());
      commitTarget.forceDelete();
    }
    log.debug("commit tmp file {} to target file {}.",
        getDmoId(), getCommitTarget().getDmoId());
    rename(commitTarget.getDmoId());
    // restore the ID in java object
    dmoId = tmpDmoId;
    return commitTarget;
  }

  public UUID uuid() {
    return this.uuid;
  }

  public DMOFile getCommitTarget() {
    return this.commitTarget;
  }

  public void swap(DMOTmpFile other) throws IOException {
    if (!StringUtils.equals(getSocketPath(), other.getSocketPath())) {
      val message = "Can only swap tmp file from same socket.";
      throw new MVFSException(message, this);
    } else if (!other.exists()) {
      val message = "Can only swap with a uncommitted tmp file";
      throw new MVFSException(message, this);
    }
    forceDelete();
    val otherUuid = other.uuid;
    val otherDmoId = other.dmoId;
    other.uuid = this.uuid;
    other.dmoId = this.dmoId;
    this.uuid = otherUuid;
    this.dmoId = otherDmoId;
  }

  @Override
  public void delete() throws IOException {
    unlink();
  }

  public static String[] listAll(String socket) throws IOException {
    return new DMOFile(socket, folderPrefix()).list();
  }

  private static String folderPrefix() {
    return String.format("/%s/", TMP_FOLDER);
  }

  public static String[] listAll() throws IOException {
    return listAll(null);
  }
}

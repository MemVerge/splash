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
package com.memverge.splash.dfs;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.memverge.splash.ShuffleFile;
import com.memverge.splash.TmpShuffleFile;
import lombok.val;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.shuffle.ShuffleSpillInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.EnumSet;
import java.util.UUID;

public class DistributedFSTmpShuffleFile extends DistributedFSShuffleFile implements TmpShuffleFile {

  private static final Logger log = LoggerFactory.getLogger(DistributedFSTmpShuffleFile.class);
  private static final DistributedFSTempFolder folder = DistributedFSTempFolder.getInstance();
  private static final String TMP_FILE_PREFIX = "tmp-";
  private DistributedFSShuffleFile commitTarget = null;
  private UUID uuid = null;

  //Taken from Spark UnsafeShuffleWriter
  private static class CloseAndFlushShieldOutputStream extends CloseShieldOutputStream {

    CloseAndFlushShieldOutputStream(OutputStream outputStream) {
      super(outputStream);
    }

    @Override
    public void flush() {
      // do nothing
    }
  }

  private DistributedFSTmpShuffleFile(String path) {
    super(path);
  }

  static DistributedFSTmpShuffleFile make() {
    val uuid = UUID.randomUUID();
    val tmpPath = folder.getTmpPath();
    val filename = String.format("%s%s", TMP_FILE_PREFIX, uuid.toString());
    val fullPath = new Path(String.format("%s/%s", tmpPath, filename));

    val ret = new DistributedFSTmpShuffleFile(fullPath.toString());
    ret.uuid = uuid;
    return ret;
  }

  static DistributedFSTmpShuffleFile make(ShuffleFile file) throws IOException {
    if (file == null) {
      throw new IOException("file is null");
    }
    if (!(file instanceof DistributedFSShuffleFile)) {
      throw new IOException("only accept DistributedFSShuffleFile");
    }
    val ret = make();
    ret.commitTarget = (DistributedFSShuffleFile) file;
    return ret;
  }

  @Override
  public TmpShuffleFile create() throws IOException {
    val fileContext = getFileContext();
    if (fileContext.util().exists(underlyingFilePath)) {
      val deleted = fileContext.delete(underlyingFilePath, true);
      log.info("file already exists.  delete file {} (result: {})",
        underlyingFilePath.toString(), deleted);
    }
    val createFlags = EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
    val createdStream = fileContext.create(underlyingFilePath, createFlags, Options.CreateOpts.createParent());
    if (createdStream == null) {
      throw new IOException(String.format("file %s already exists.", getPath()));
    } else {
      createdStream.close();
      log.debug("file {} created.", getPath());
    }
    return this;
  }

  @Override
  public void swap(TmpShuffleFile other) throws IOException {
    if (!other.exists()) {
      val message = "Can only swap with a uncommitted tmp file";
      throw new IOException(message);
    }

    val otherFile = (DistributedFSTmpShuffleFile) other;

    delete();

    val tmpUuid = otherFile.uuid;
    otherFile.uuid = this.uuid;
    this.uuid = tmpUuid;

    val tmpFilePath = this.underlyingFilePath;
    this.underlyingFilePath = otherFile.underlyingFilePath;
    otherFile.underlyingFilePath = tmpFilePath;
  }

  @Override
  public DistributedFSShuffleFile getCommitTarget() {
    return this.commitTarget;
  }

  @Override
  public ShuffleFile commit() throws IOException {
    if (commitTarget == null) {
      throw new IOException("No commit target.");
    } else if (!exists()) {
      create();
    }
    if (commitTarget.exists()) {
      val msg = String.format("commit target %s already exists", commitTarget.getPath());
      log.warn(msg);
      throw new FileAlreadyExistsException(msg);
    }
    log.debug("commit tmp file {} to target file {}.",
      getPath(), getCommitTarget().getPath());

    rename(commitTarget.getPath());
    return commitTarget;
  }

  @Override
  public void recall() {
    val commitTarget = getCommitTarget();
    if (commitTarget != null) {
      log.info("recall tmp file {} of target file {}.",
        getPath(), commitTarget.getPath());
    } else {
      log.info("recall tmp file {} without target file.", getPath());
    }

    try {
      delete();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public UUID uuid() {
    return this.uuid;
  }

  @Override
  public OutputStream makeOutputStream() {
    val fileContext = getFileContext();
    val createFlags = EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
    OutputStream ret;
    try {
      ret = fileContext.create(underlyingFilePath, createFlags, Options.CreateOpts.createParent());
      log.debug("create output stream for {}.", getPath());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    return ret;
  }

  @Override
  public int getBufferSize() {
    val fileContext = getFileContext();
    try {
      return Long.valueOf(fileContext.getFileStatus(underlyingFilePath).getLen()).intValue();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public long[] fastMerge(ShuffleSpillInfo[] spills) throws IOException {
    assert (spills.length >= 2);
    val numPartitions = spills[0].partitionLengths().length;
    val partitionLengths = new long[numPartitions];
    val spillInputStreams = new InputStream[spills.length];

    val fileContext = getFileContext();
    val createFlags = EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
    val bos = fileContext.create(underlyingFilePath, createFlags, Options.CreateOpts.createParent());
    // Use a counting output stream to avoid having to close the underlying file and ask
    // the file system for its size after each partition is written.
    val mergedFileOutputStream = new CountingOutputStream(bos);

    boolean threwException = true;
    try {
      for (int i = 0; i < spills.length; i++) {
        val shuffleTmpFile = (DistributedFSTmpShuffleFile) spills[i].file();
        spillInputStreams[i] = fileContext.open(shuffleTmpFile.underlyingFilePath);
      }

      for (int partition = 0; partition < numPartitions; partition++) {
        final long initialFileLength = mergedFileOutputStream.getByteCount();
        // Shield the underlying output stream from close() and flush() calls, so that we can close
        // the higher level streams to make sure all data is really flushed and internal state is
        // cleaned.
        val partitionOutput = new CloseAndFlushShieldOutputStream(mergedFileOutputStream);
        for (int i = 0; i < spills.length; i++) {
          val partitionLengthInSpill = spills[i].partitionLengths()[partition];
          if (partitionLengthInSpill > 0) {
            try (InputStream partitionInputStream = new LimitedInputStream(spillInputStreams[i],
              partitionLengthInSpill, false)) {
              ByteStreams.copy(partitionInputStream, partitionOutput);
            }
          }
        }
        partitionOutput.flush();
        partitionOutput.close();
        partitionLengths[partition] = (mergedFileOutputStream.getByteCount() - initialFileLength);
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (InputStream stream: spillInputStreams) {
        Closeables.close(stream, threwException);
      }
      Closeables.close(mergedFileOutputStream, threwException);
    }
    return partitionLengths;
  }
}

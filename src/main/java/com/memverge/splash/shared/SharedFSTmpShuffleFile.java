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
package com.memverge.splash.shared;

import com.google.common.io.Closeables;
import com.memverge.splash.ShuffleFile;
import com.memverge.splash.TempFolder;
import com.memverge.splash.TmpShuffleFile;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Paths;
import java.util.UUID;
import lombok.val;
import org.apache.spark.shuffle.ShuffleSpillInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedFSTmpShuffleFile extends SharedFSShuffleFile implements
    TmpShuffleFile {

  private static final Logger log = LoggerFactory
      .getLogger(SharedFSTmpShuffleFile.class);

  private static TempFolder folder = TempFolder.getInstance();

  private static final String TMP_FILE_PREFIX = "tmp-";
  private SharedFSShuffleFile commitTarget = null;

  private UUID uuid = null;

  private SharedFSTmpShuffleFile(String pathname) {
    super(pathname);
  }

  static SharedFSTmpShuffleFile make() {
    val uuid = UUID.randomUUID();
    val tmpPath = folder.getTmpPath();
    val filename = String.format("%s%s", TMP_FILE_PREFIX, uuid.toString());
    val fullPath = Paths.get(tmpPath, filename).toString();

    val ret = new SharedFSTmpShuffleFile(fullPath);
    ret.uuid = uuid;
    return ret;
  }

  static SharedFSTmpShuffleFile make(ShuffleFile file) throws IOException {
    if (file == null) {
      throw new IOException("file is null");
    }
    if (!(file instanceof SharedFSShuffleFile)) {
      throw new IOException("only accept SharedFSShuffleFile");
    }
    val ret = make();
    ret.commitTarget = (SharedFSShuffleFile) file;
    return ret;
  }

  @Override
  public TmpShuffleFile create() throws IOException {
    val file = getFile();
    val parent = file.getParentFile();
    if (!parent.exists()) {
      boolean created = parent.mkdirs();
      if (!created) {
        log.info("parent folder {} creation return false.",
            parent.getAbsolutePath());
      }
    }
    if (file.exists()) {
      val deleted = file.delete();
      log.info("file already exists.  delete file {} (result: {})",
          file.getAbsolutePath(), deleted);
    }
    val created = file.createNewFile();
    if (!created) {
      throw new IOException(
          String.format("file %s already exists.", getPath()));
    } else {
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

    val otherLocal = (SharedFSTmpShuffleFile) other;

    delete();

    val tmpUuid = otherLocal.uuid;
    otherLocal.uuid = this.uuid;
    this.uuid = tmpUuid;

    val tmpFile = this.file;
    this.file = otherLocal.file;
    otherLocal.file = tmpFile;
  }

  @Override
  public SharedFSShuffleFile getCommitTarget() {
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
      val msg = String.format("commit target %s already exists",
          commitTarget.getPath());
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
    delete();
  }

  @Override
  public OutputStream makeOutputStream() {
    try {
      create();
    } catch (IOException e) {
      val msg = String.format("Create file %s failed.", getPath());
      throw new IllegalArgumentException(msg, e);
    }
    OutputStream ret;
    try {
      ret = new FileOutputStream(file, false);
      log.debug("create output stream for {}.", getPath());
    } catch (FileNotFoundException e) {
      val msg = String.format("File %s not found?", getPath());
      throw new IllegalArgumentException(msg, e);
    }
    return ret;
  }

  @Override
  public UUID uuid() {
    return this.uuid;
  }

  @Override
  public boolean supportFastMerge() {
    return true;
  }

  @Override
  public long[] fastMerge(ShuffleSpillInfo[] spills) throws IOException {
    // fast merge using jni channel support.
    // this function is migrated from the original Spark's
    // UnsafeShuffleWriter.mergeSpillsWithTransferTo
    // other storage plugin could implement their own fast merge.
    //
    // Note: The performance of this function could seriously impact the
    // performance of Spark SQL joins with multiple spills.
    assert (spills.length >= 2);
    val numPartitions = spills[0].partitionLengths().length;
    val partitionLengths = new long[numPartitions];
    val spillInputChannels = new FileChannel[spills.length];
    val spillInputChannelPositions = new long[spills.length];
    FileChannel mergedFileOutputChannel = null;

    boolean threwException = true;
    try {
      for (int i = 0; i < spills.length; i++) {
        val f = (SharedFSTmpShuffleFile) spills[i].file();
        spillInputChannels[i] = new FileInputStream(f.file).getChannel();
      }
      // This file needs to opened in append mode in order to work around a Linux kernel bug that
      // affects transferTo; see SPARK-3948 for more details.
      mergedFileOutputChannel = new FileOutputStream(this.file, true)
          .getChannel();

      long bytesWrittenToMergedFile = 0;
      for (int partition = 0; partition < numPartitions; partition++) {
        for (int i = 0; i < spills.length; i++) {
          val partitionLengthInSpill = spills[i].partitionLengths()[partition];
          val spillInputChannel = spillInputChannels[i];
          copyFileStreamNIO(
              spillInputChannel,
              mergedFileOutputChannel,
              spillInputChannelPositions[i],
              partitionLengthInSpill);
          spillInputChannelPositions[i] += partitionLengthInSpill;
          bytesWrittenToMergedFile += partitionLengthInSpill;
          partitionLengths[partition] += partitionLengthInSpill;
        }
      }
      // Check the position after transferTo loop to see if it is in the right position and raise an
      // exception if it is incorrect. The position will not be increased to the expected length
      // after calling transferTo in kernel version 2.6.32. This issue is described at
      // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.
      if (mergedFileOutputChannel.position() != bytesWrittenToMergedFile) {
        throw new IOException(
            "Current position " + mergedFileOutputChannel.position()
                + " does not equal expected "
                + "position "
                + bytesWrittenToMergedFile
                + " after transferTo. Please check your kernel"
                + " version to see if it is 2.6.32, "
                + "as there is a kernel bug which will lead to "
                + "unexpected behavior when using transferTo. "
                + "You can set spark.file.transferTo=false "
                + "to disable this NIO feature."
        );
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (int i = 0; i < spills.length; i++) {
        Closeables.close(spillInputChannels[i], threwException);
      }
      Closeables.close(mergedFileOutputChannel, threwException);
    }
    return partitionLengths;
  }

  private void copyFileStreamNIO(
      FileChannel input,
      FileChannel output,
      long startPosition,
      long bytesToCopy) throws IOException {
    long count = 0L;
    while (count < bytesToCopy) {
      count += input.transferTo(
          count + startPosition,
          bytesToCopy - count,
          output);
    }
  }
}

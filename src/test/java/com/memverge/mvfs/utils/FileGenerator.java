/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package com.memverge.mvfs.utils;

import com.memverge.mvfs.dmo.DMOFile;
import com.memverge.mvfs.dmo.DMOOutputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Random;
import java.util.zip.CRC32;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class FileGenerator {

  private final TmpFileFolder tmpFileFolder;


  public FileGenerator() {
    tmpFileFolder = new TmpFileFolder("dmoTestFiles");
  }

  private File getNewFile(String name) throws IOException {
    val file = getFile(name);
    val folder = file.getParentFile();
    if (!folder.exists()) {
      folder.mkdirs();
    }
    file.delete();
    file.createNewFile();
    return file;
  }

  private File getFile(String name) {
    return Paths.get(tmpFileFolder.getTmpPath(), name).toFile();
  }

  private OutputStream getFileOs(String name) throws IOException {
    return new BufferedOutputStream(new FileOutputStream(getNewFile(name)));
  }

  private DMOOutputStream getDmoOs(String name, int chunkSize) throws IOException {
    val dmoFile = new DMOFile("", name, chunkSize);
    return new DMOOutputStream(dmoFile, false, true);
  }

  public File genFile(String name, String content) throws IOException {
    try (val dmoOs = getFileOs(name)) {
      val buf = content.getBytes();
      dmoOs.write(buf, 0, buf.length);
    }
    return getFile(name);
  }

  public FileInfo genDmoFile(String name, String content) throws IOException {
    val crcMaker = new CRC32();
    try (val dmoOs = getDmoOs(name, 256 * 1024)) {
      val buf = content.getBytes();
      dmoOs.write(buf, 0, buf.length);
      crcMaker.update(buf, 0, buf.length);
    }
    return new FileInfo(name, crcMaker.getValue(), content.length());
  }

  public FileInfo genRandomDmoFile(String name, long size) throws IOException {
    return genRandomDmoFile(name, 2 * 1024 * 1024, size);
  }

  public FileInfo genRandomDmoFile(String name, int chunkSize, long size)
      throws IOException {
    val crcMaker = new CRC32();
    long written = 0;
    try (val dmoOs = getDmoOs(name, chunkSize)) {
      val rand = new Random();
      val iSize = (int) size;
      val buf = new byte[iSize];
      rand.nextBytes(buf);
      dmoOs.write(buf, written, iSize);
      crcMaker.update(buf, 0, iSize);
    } catch (IOException e) {
      log.error("write random file {} failed.", name, e);
      throw e;
    }
    return new FileInfo(name, crcMaker.getValue(), size);
  }
}

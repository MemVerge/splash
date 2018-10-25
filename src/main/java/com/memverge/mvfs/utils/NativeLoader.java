/*
 * Copyright (c) 2018 MemVerge Inc.
 */

package com.memverge.mvfs.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.io.IOUtils;

@Slf4j
public class NativeLoader {

  private final String libName;

  public NativeLoader(String libName) {
    this.libName = libName;
  }

  public void load() {
    val localLib = saveLibFromRsc();
    if (localLib == null) {
      log.warn("could not find library {} in package resource.  " +
          "fall back to System.loadLibrary.", libName);
      System.loadLibrary(libName);
    } else {
      System.load(localLib);
    }
  }

  String saveLibFromRsc() {
    String ret = null;
    try {
      val libFile = File.createTempFile(getPrefix(), ".tmp.so", getOutputFolder());
      libFile.deleteOnExit();
      if (copyLibFile(libFile)) {
        ret = libFile.getAbsolutePath();
      }
    } catch (IOException ex) {
      log.error("failed to create temp library file.", ex);
    }
    return ret;
  }

  private String getPrefix() {
    String ret = libName;
    if (libName.length() < 3) {
      ret += "lib";
    }
    return ret;
  }

  private boolean copyLibFile(File libFile) {
    boolean ret = false;
    val libName = getLibName();
    log.info("start copying lib: {}", libName);
    try (
        val libIs = getClass().getClassLoader().getResourceAsStream(libName);
        val libOs = new FileOutputStream(libFile)
    ) {
      IOUtils.copy(libIs, libOs);
      log.info("copy tmp lib file {} success", libFile.getAbsolutePath());
      ret = true;
    } catch (NullPointerException ex) {
      log.error("failed to find lib in resources: {}", libName);
    } catch (IOException ex) {
      log.error("failed to copy platform native lib.", ex);
    }
    return ret;
  }

  private String getLibName() {
    val os = getPropSafe("os.name").toLowerCase();
    val arch = getPropSafe("os.arch");
    val libFolder = String.format("%s-%s", os, arch);
    String filename = libName;
    if (os.startsWith("linux")) {
      filename = String.format("lib%s.so", filename);
    }
    return Paths.get("lib", libFolder, filename).toString();
  }

  private File getOutputFolder() {
    val tmpFolderName = getPropSafe("java.io.tmpdir");
    val tempFolder = new File(tmpFolderName);
    if (!tempFolder.exists()) {
      tempFolder.mkdirs();
    }
    return tempFolder;
  }

  private String getPropSafe(String propName) {
    val ret = System.getProperty(propName);
    return ret == null ? "NA" : ret;
  }
}

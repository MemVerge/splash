package com.memverge.splash.local;

import com.memverge.splash.ShuffleFile;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalShuffleFile implements ShuffleFile {

  String localId;

  private static final Logger log = LoggerFactory
      .getLogger(LocalShuffleFile.class);

  protected File file;

  private static String fullPath = null;

  public LocalShuffleFile(String pathname)
      throws IOException {

    if(!new File(new File(pathname).getParent()).exists()) {
      new File(new File(pathname).getParent()).mkdirs();
    }

    file = new File(formalizeId(pathname));
    localId = file.getAbsolutePath();
    if(fullPath == null) {
      fullPath = file.getParent().toString();
    }
  }

  public LocalShuffleFile() {
  }

  @Override
  public ShuffleFile create() throws IOException {
    // TODO: Update this if it doesn't suit your needs
    boolean created = getFile().createNewFile();
    if (!created) {
      log.warn("file {} already exists.", file.getAbsolutePath());
    }
    return this;
  }

  @Override
  public long getSize() {
    return file.length();
  }

  public String getLocalId() {
    return file.getAbsolutePath();
  }

  @Override
  public boolean delete() throws IOException{
    if(file == null) {
     throw new IOException("file is null");
    }
    return file.delete();
  }

  @Override
  public boolean exists() {
    return file.exists();
  }

  @Override
  public String[] list() {
    List<String> ret = new ArrayList<>();
    listAll(file,ret);
    return ret.toArray(new String[0]);
  }

  public void listAll(File CurrentFile, List<String> files) {
    File[] fList = CurrentFile.listFiles();
    if(fList != null)
      for (File eachFile : fList) {
        if (eachFile.isFile()) {
          files.add(eachFile.getAbsolutePath());
        } else if (eachFile.isDirectory()) {
          listAll(eachFile, files);
        }
      }
  }

  @Override
  public String getId() {
    // TODO: Update this if it doesn't suit your needs
    return file.getAbsolutePath();
  }

  //TODO:Done
  @Override
  public void reset() {
    try {
      new LocalStorageFactory().getDataFile(fullPath).delete();
    } catch (IOException e) {
      log.debug("files for '{}' already cleared.", file.getAbsolutePath());
      log.warn("Reset failed.", e);
    }
  }

  @Override
  public InputStream makeInputStream() {
    // TODO: Need to test this
    InputStream ret;
    try {
      ret = new FileInputStream(file);
    } catch (FileNotFoundException e) {
      String msg = String.format("Create input stream failed for %s.",
          file.getAbsolutePath());
      throw new IllegalArgumentException(msg, e);
    }
    return ret;
  }

  @Override
  public OutputStream makeOutputStream(boolean append, boolean create) {
    // TODO: Need to test this
    if (!exists()) {
      if (create) {
        try {
          create();
        } catch (IOException e) {
          String msg = String.format("Create file %s failed.", getId());
          throw new IllegalArgumentException(msg, e);
        }
      } else {
        String msg = String.format("%s not found.", getId());
        throw new IllegalArgumentException(msg);
      }
    }
    OutputStream ret;
    try {
      ret = new FileOutputStream(file, append);
    } catch (FileNotFoundException e) {
      String msg = String.format("File %s not found?", getId());
      throw new IllegalArgumentException(msg, e);
    }
    return ret;
  }

  private File getFile() {
    return file;
  }

  private String formalizeId(String id) throws IOException {
    return id;
  }

  public void forceDelete() {
    try {
      if (exists()) {
        delete();
      }
    } catch (IOException ex) {
      log.warn("Error while deleting '{}'.  Ignore it due to force.", localId);
    }
  }
  //TODO:Done -- please take notice of this, I omit some exception message
  public boolean rename(String tgtId) throws IOException {
    new File(new File(tgtId).getParent()).mkdirs();
    boolean rc = file.renameTo(Paths.get(tgtId).toFile());
    if (rc) {
      log.debug("rename {} to {}.", localId, tgtId);
      localId = formalizeId(tgtId);
    } else {
      throw new IOException("Rename File to " + tgtId + " failed.");
    }
    return rc;
  }

  public static void deleteAll() {
    deleteDir(new File(new LocalTmpFileFolder("/").getTmpPath()));
  }

  private static boolean deleteDir(File dir) {
    if (dir.isDirectory()) {
      String[] children = dir.list();
      for (int i=0; i<children.length; i++) {
        boolean success = deleteDir(new File(dir, children[i]));
        if (!success) {
          return false;
        }
      }
    }
    return dir.delete();
  }

}

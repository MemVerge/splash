package com.memverge.splash.local;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

@Slf4j
public class LocalTmpFileFolder {

  private static Map<String, LocalTmpFileFolder> folders = new ConcurrentHashMap<>();
  private final String[] folder;

  public LocalTmpFileFolder(String... folder) {
    this.folder = folder;
    folders.putIfAbsent(getTmpPath(), this);

  }

  public static void cleanup() {
    folders.values().forEach(LocalTmpFileFolder::remove);
  }

  public String getTmpPath() {
    val tmpFolder = System.getProperty("java.io.tmpdir");
    val path = Paths.get(tmpFolder, getUser());
    return Paths.get(path.toString(), folder).toString();
  }

  private String getUser() {
    val user = System.getProperty("user.name");
    return StringUtils.isEmpty(user) ? "unknown" : user;
  }

  public String getRelativePath(String path) {
    return path.replace(getTmpPath(), "")
        .replaceAll(Pattern.quote(String.valueOf(File.separatorChar)), "/");
  }

  private void remove() {
    val path = getTmpPath();
    try {
      FileUtils.forceDelete(new File(getTmpPath()));
    } catch (FileNotFoundException e) {
      // do nothing
    } catch (IOException e) {
      log.error("failed to clean up tmp folder: {}", path, e);
    }
  }
}

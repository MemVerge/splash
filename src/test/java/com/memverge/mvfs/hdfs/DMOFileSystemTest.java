package com.memverge.mvfs.hdfs;

import static com.memverge.mvfs.TestGroups.UT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.memverge.mvfs.connection.MVFSConnectOptions;
import com.memverge.mvfs.dmo.DMOFile;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Random;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = UT)
public class DMOFileSystemTest {

  private DMOFileSystem dmoFs;

  @BeforeMethod
  private void beforeMethod() throws URISyntaxException, IOException {
    dmoFs = new DMOFileSystem();
    dmoFs.initialize(new URI(toUrl("/")), getConf());
  }

  @AfterMethod
  private void afterMethod() {
    DMOFile.safeReset();
  }

  private Configuration getConf() {
    return new Configuration();
  }

  public void testInitializeDefaultUri() throws IOException {
    val fs = new DMOFileSystem();
    fs.initialize(null, getConf());
    val socket = MVFSConnectOptions.defaultSocket();
    val url = String.format("dmo://%s/", socket);

    assertThat(fs.getSocket()).isEqualTo(socket);
    assertThat(fs.getUri().toString()).isEqualTo(url);
    assertThat(fs.getWorkingDirectory().toString()).isEqualTo(url);
  }

  public void testConfigSocket() throws IOException {
    val conf = getConf();
    conf.set("mvfs.socket", "1");
    val fs = new DMOFileSystem();
    fs.initialize(null, conf);
    assertThat(fs.getUri().toString()).isEqualTo("dmo://1/");
  }

  public void testMakeAbsolute() {
    val path = dmoFs.makeAbsolute(new Path("a/b"));
    assertThat(path.toString()).isEqualTo(toUrl("/a/b"));
  }

  public void testGetDmoFile() throws IOException {
    val path = new Path(toUrl("/license"));
    val dmoFile = dmoFs.getDmoFile(path);
    assertThat(dmoFile.getDmoId()).isEqualTo("/license");
  }

  public void testGetFileStatusNotFound() {
    assertThatExceptionOfType(FileNotFoundException.class)
        .isThrownBy(() -> dmoFs.getFileStatus(new Path(toUrl("/NA"))));
  }

  public void testListStatusNotFound() {
    assertThatExceptionOfType(FileNotFoundException.class)
        .isThrownBy(() -> dmoFs.listStatus(new Path(toUrl("/NA"))));
  }

  public void testListStatusForFile() throws IOException {
    val path = new Path(toUrl("/me.txt"));
    try (val os = dmoFs.create(path)) {
      os.writeBytes("about me.");
    }
    val statusList = dmoFs.listStatus(path);
    assertThat(statusList).hasSize(1);
    val status = statusList[0];
    assertThat(status.getLen()).isEqualTo(9);
  }

  public void testListStatusForFolder() throws IOException {
    addFile(toUrl("/a/123.txt"), "123");
    addFile(toUrl("/a/4567.txt"), "4567");
    addFile(toUrl("/aaa.txt"), "aaa");
    addFile(toUrl("/b/890.txt"), "890");
    val path = new Path(toUrl("/a"));
    val statusList = dmoFs.listStatus(path);
    assertThat(statusList).hasSize(2);
    Arrays.sort(statusList, (a, b) -> (int) (a.getLen() - b.getLen()));
    val s0 = statusList[0];
    assertThat(s0.getLen()).isEqualTo(3);
    assertThat(s0.getPath().toString()).isEqualTo(toUrl("/a/123.txt"));
    val s1 = statusList[1];
    assertThat(s1.getLen()).isEqualTo(4);
    assertThat(s1.getPath().toString()).isEqualTo(toUrl("/a/4567.txt"));
  }

  public void testListStatusContainsSubFolder() throws IOException {
    addFile(toUrl("/a/aa/123.txt"), "123");
    addFile(toUrl("/a/4567.txt"), "4567");
    val path = new Path("a");
    assertThat(dmoFs.listStatus(path)).hasSize(2);
  }

  public void testRename() throws IOException {
    val text = "text in source";
    val src = toUrl("/src.txt");
    val tgt = toUrl("/tgt.txt");
    addFile(src, text);
    dmoFs.rename(new Path(src), new Path(tgt));
    val tgtFile = dmoFs.getDmoFile(new Path(tgt));
    try (val is = new HdfsDmoInputStream(tgtFile, null)) {
      val buf = new byte[(int) tgtFile.getSize()];
      IOUtils.readFully(is, buf);
      val tgtText = new String(buf);
      assertThat(tgtText).isEqualTo(text);
    }
    val srcFile = dmoFs.getDmoFile(new Path(src));
    assertThat(srcFile.exists()).isFalse();
  }

  public void testRenameFolder() throws IOException {
    addFile(toUrl("/x/y/z.txt"), "zzz");
    dmoFs.rename(new Path(toUrl("/x")), new Path(toUrl("/a")));
    verifyExists(toUrl("/a"));
    verifyExists(toUrl("/a/y"));
    verifyExists(toUrl("/a/y/z.txt"));
    verifyNotExists(toUrl("/x"));
    verifyNotExists(toUrl("/x/y"));
    verifyNotExists(toUrl("/x/y/z.txt"));
    verifyIsDirectory(toUrl("/a"));
  }

  public void testDeleteFileNotExists() throws IOException {
    dmoFs.delete(new Path(toUrl("/abc")), true);
    verifyNotExists(toUrl("/abc"));
  }

  public void testMKDirs() throws IOException {
    val path = new Path("x/y/z");
    val ret = dmoFs.mkdirs(path, null);
    assertThat(ret).isTrue();
    verifyExists("/x/y/z/");
  }

  public void testIsRoot() {
    assertThat(dmoFs.isRoot("dmo://x/")).isTrue();
  }

  public void testIsRootFalse() {
    assertThat(dmoFs.isRoot(toUrl("/1"))).isFalse();
  }

  public void testMKDirsRecursive() throws IOException {
    String pathStr = toUrl("/m/n/l");
    addDir(pathStr);
    verifyExists(pathStr);
    verifyExists(toUrl("/m/n"));
    verifyIsDirectory(toUrl("/m/n"));
    verifyExists(toUrl("/m"));
  }

  private void verifyExists(String pathStr) throws IOException {
    assertThat(dmoFs.exists(new Path(pathStr)))
        .as("%s should exist.").isTrue();
  }

  private void verifyNotExists(String pathStr) throws IOException {
    assertThat(dmoFs.exists(new Path(pathStr)))
        .as("%s should not exist.", pathStr).isFalse();
  }

  private void verifyIsDirectory(String pathStr) throws IOException {
    assertThat(dmoFs.isDirectory(new Path(pathStr)))
        .as("%s is directory", pathStr).isTrue();
  }

  public void testAddWithRelativePath() throws IOException {
    addFile("readme.txt", "read me");
    val absolutePath = new Path(toUrl("/readme.txt"));
    assertThat(dmoFs.exists(absolutePath)).isTrue();
  }

  public void testFolderDmoIdEndsWithSlash() throws IOException {
    addDir(toUrl("/a/b/c"));
    val a = new DMOFile("/a/");
    val b = new DMOFile("/a/b/");
    val c = new DMOFile("/a/b/c/");
    assertThat(a.exists()).isTrue();
    assertThat(b.exists()).isTrue();
    assertThat(c.exists()).isTrue();
    verifyIsDirectory(toUrl("/a/b/c/"));
  }

  public void testAddFileCreateFoldersAllTheWay() throws IOException {
    val content = "content in c";
    addFile("/a/b/c.txt", content);
    assertThat(new DMOFile("/a").exists()).isTrue();
    assertThat(new DMOFile("/a/b").exists()).isTrue();
    assertThat(new DMOFile("/a/b/c.txt").exists()).isTrue();
  }

  public void testDeleteDirRecursive() throws IOException {
    val mnDir = toUrl("/m/n");
    val mnlDir = toUrl("/m/n/l");
    val mnFile = toUrl("/m/mn.txt");
    val mDir = toUrl("/m");
    addDir(mnDir);
    addDir(mnlDir);
    addFile(mnFile, "m & n");
    dmoFs.delete(new Path(mDir), true);
    assertThat(dmoFs.exists(new Path(mnFile))).isFalse();
    assertThat(dmoFs.exists(new Path(mnlDir))).isFalse();
    assertThat(dmoFs.exists(new Path(mnDir))).isFalse();
    assertThat(dmoFs.exists(new Path(mDir))).isFalse();
  }

  @Test(timeOut = 1000)
  public void testBufferedWrite() throws IOException {
    try (val os = dmoFs.create(new Path("testBufferedWrite"))) {
      val rand = new Random();
      for (int i = 0; i < 5000000; i++) {
        os.write(rand.nextInt());
      }
    }
  }

  private void addDir(String pathStr) throws IOException {
    val path = new Path(pathStr);
    dmoFs.mkdirs(path, null);
  }

  private void addFile(String pathStr, String content) throws IOException {
    val path = new Path(pathStr);
    try (val os = dmoFs.create(path)) {
      os.writeBytes(content);
    }
  }

  private String toUrl(String path) {
    val socket = MVFSConnectOptions.defaultSocket();
    return String.format("dmo://%s%s", socket, path);
  }
}

/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package com.memverge.mvfs.dmo;

import static com.memverge.mvfs.TestGroups.IT;
import static com.memverge.mvfs.TestGroups.UT;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.within;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.memverge.mvfs.error.MVFSException;
import com.memverge.mvfs.utils.FileGenerator;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import lombok.val;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {UT, IT})
public class DMOFileTest {

  @BeforeMethod
  private void beforeMethod() {
    afterMethod();
  }

  @AfterMethod
  private void afterMethod() {
    DMOFile.safeReset();
  }

  private DMOFile createDmo(String id, String content) throws IOException {
    val file = new DMOFile(id).create();
    if (!file.isFolder()) {
      try (val os = new OutputStreamWriter(new DMOOutputStream(file))) {
        os.write(content);
      }
    }
    return file;
  }

  private DMOFile createDmo(String id) throws IOException {
    return createDmo(id, id + " content!");
  }

  private List<DMOFile> createDmo(Collection<String> idList)
      throws IOException {
    val ret = new ArrayList<DMOFile>();
    for (val id : idList) {
      ret.add(createDmo(id));
    }
    return ret;
  }

  public void testReset() throws IOException {
    val a1 = createDmo("a1");
    val a2 = createDmo("a2");
    assertThat(a1.exists()).isTrue();
    assertThat(a2.exists()).isTrue();

    DMOFile.reset();
    assertThat(a1.exists()).isFalse();
    assertThat(a2.exists()).isFalse();
  }

  public void testListAll() throws IOException {
    assertThat(createDmo(asList("b1", "b2")).size()).isEqualTo(2);
    val list = DMOFile.listAll();
    assertThat(list).containsOnly("/b1", "/b2");
  }

  public void testListAllWithFolder() throws IOException {
    createDmo(asList("/a/b/c.txt", "/d/e.jpg", "f"));
    assertThat(DMOFile.listAll())
        .containsOnly("/a/", "/a/b/", "/a/b/c.txt", "/d/", "/d/e.jpg", "/f");
  }

  public void testIsDir() throws IOException {
    val dmoDir = createDmo("/ab/");
    assertThat(dmoDir.isFolder()).isTrue();

    val dmoFile = createDmo("/ac");
    assertThat(dmoFile.isFolder()).isFalse();
  }

  public void testAttribute() throws IOException {
    val str = "hello";
    val chunkSize = 64 * 1024;
    val dmoFile = new DMOFile("", "testAttribute", chunkSize);
    val timestamp = new Date().toInstant().getEpochSecond();
    try (val dmoOs = new DMOOutputStream(dmoFile, false, true)) {
      dmoOs.write(str.getBytes());
    }
    val attr = dmoFile.attribute();
    assertThat(attr.getRc()).isGreaterThanOrEqualTo(0);
    assertThat(attr.getSize()).isEqualTo((long) str.length());
    assertThat(attr.getChunkSize()).isEqualTo(chunkSize);
    assertThat(attr.getBTime()).isCloseTo(timestamp, within(10L));
    assertThat(attr.getMTime()).isCloseTo(timestamp, within(10L));
    assertThat(attr.getATime()).isCloseTo(timestamp, within(10L));
  }

  public void testFileNotExists() throws IOException {
    val dmoFile = new DMOFile("notExists");
    assertThat(dmoFile.exists()).isFalse();
  }

  public void testFileExists() throws IOException {
    val dmoFile = new DMOFile("existedFile");
    try {
      dmoFile.create();
      assertThat(dmoFile.exists()).isTrue();
    } finally {
      dmoFile.delete();
    }
  }

  public void testWriteFromFile() throws IOException {
    val fileGen = new FileGenerator();
    val content = "demo content";
    val name = "testWriteFromFile";
    val file = fileGen.genFile(name, content);
    val dmoFile = new DMOFile(name).create();
    dmoFile.write(file);
    assertThat(new String(dmoFile.read())).isEqualTo(content);
  }

  public void testListRoot() throws IOException {
    new DMOFile("file1").create();
    new DMOFile("file2").create();
    new DMOFile("file3").create();
    assertThat(new DMOFile("/").list())
        .containsOnly("file1", "file2", "file3");
    assertThat(new DMOFile("/").list())
        .containsOnly("file1", "file2", "file3");
  }

  public void testListLeafFolder() throws IOException {
    new DMOFile("/a/b/c.txt").create();
    new DMOFile("/a/b/c.jpg").create();
    new DMOFile("/d/e/f.txt").create();
    assertThat(new DMOFile("/a/b/").list())
        .containsOnly("c.txt", "c.jpg");
    assertThat(new DMOFile("/d/e/").list())
        .containsOnly("f.txt");
  }

  public void testListChild() throws IOException {
    createDmo(asList("/a/aa/aaa", "/b", "c", "/a/ab"));
    val file = new DMOFile("/a/");
    assertThat(file.list()).containsOnly("aa/", "ab");
  }

  public void testListChildWithSubFolder() throws IOException {
    createDmo(asList("/a/4567.txt", "/a/aa/123.txt"));
    val file = new DMOFile("/a/");
    assertThat(file.list()).containsOnly("4567.txt", "aa/");
  }

  public void testDefaultChunkSize() throws IOException {
    val dmoFile = createDmo("testDefaultChunkSize");
    assertThat(dmoFile.attribute().getChunkSize()).isEqualTo(1024 * 1024);
  }

  public void testSimpleRename() throws IOException {
    val src = "/a/b";
    val tgt = "/a/c";
    val file = createDmo(src);
    file.rename(tgt);
    assertThat(file.getDmoId()).isEqualTo(tgt);
    assertThat(file.exists()).isTrue();
    assertThat(new DMOFile(src).exists()).isFalse();
  }

  public void testRenameCreatePaths() throws IOException {
    val src = "/a/b";
    val tgt = "/1/2/3";
    val file = createDmo(src);
    file.rename(tgt);
    assertThat(file.getDmoId()).isEqualTo(tgt);
    assertThat(file.exists()).isTrue();
    assertThat(new DMOFile("/1/").exists()).isTrue();
    assertThat(new DMOFile("/1/2/").exists()).isTrue();
    assertThat(new DMOFile(src).exists()).isFalse();
  }

  public void testRenameToHigher() throws IOException {
    createDmo(asList("/a/b/", "/d/"));
    val src = "/a/b/c";
    val tgt = "/d/e";
    val file = createDmo(src);
    file.rename(tgt);
    assertThat(new DMOFile(src).exists()).isFalse();
    assertThat(file.exists()).isTrue();
    assertThat(file.getDmoId()).isEqualTo(tgt);
    assertThat(new DMOFile(tgt).exists()).isTrue();
  }

  public void testRenameToLower() throws IOException {
    createDmo("/a/b/");
    val src = "/d";
    val tgt = "/a/b/c";
    val file = createDmo(src);
    file.rename(tgt);
    assertThat(new DMOFile(src).exists()).isFalse();
    assertThat(file.exists()).isTrue();
    assertThat(file.getDmoId()).isEqualTo(tgt);
    assertThat(new DMOFile(tgt).exists()).isTrue();
  }

  public void testMkdir() throws IOException {
    val folder = createDmo("/a/b/");
    assertThat(folder.isFolder()).isTrue();
    assertThat(folder.exists()).isTrue();
    assertThat(new DMOFile("/a/").list()).containsOnly("b/");
  }

  public void testRmdirWithDelete() throws IOException {
    createDmo(asList("/a/b/c.txt", "/a/b/d/e.jpg"));
    val folder = new DMOFile("/a/b");
    folder.delete();
    assertThat(folder.exists()).isFalse();
    assertThat(new DMOFile("/a").list()).hasSize(0);
    assertThat(new DMOFile("/a/b/c.txt").exists()).isFalse();
    assertThat(new DMOFile("/a/b").exists()).isFalse();
  }

  public void testRmdir() throws IOException {
    createDmo(asList("/a/b/c.txt", "/a/d.txt"));
    val folder = new DMOFile("/a");
    folder.rmdir();
    assertThat(folder.exists()).isFalse();
    assertThat(new DMOFile("/a").list()).hasSize(0);
    assertThat(new DMOFile("/a/b/c.txt").exists()).isFalse();
    assertThat(new DMOFile("/a/d.txt").exists()).isFalse();
  }

  public void testRmdirOnFile() throws IOException {
    val dmoFile = createDmo("rmdirOnFile");
    assertThatExceptionOfType(MVFSException.class).isThrownBy(dmoFile::rmdir);
  }

  @Test(enabled = false)
  public void testUnlinkNotEmptyFolderShouldFail() throws IOException {
    createDmo(asList("/a/b/c.txt", "/a/d.txt"));
    val folder = new DMOFile("/a");
    assertThatExceptionOfType(MVFSException.class).isThrownBy(folder::unlink);
  }

  public void testForceDelete() throws IOException {
    createDmo(asList("/a/b/c/1.txt", "/a/b/c/2.jpg"));
    val folder = new DMOFile("/a/b");
    folder.forceDelete();
    assertThat(new DMOFile("/a/b").exists()).isFalse();
    assertThat(new DMOFile("/a").exists()).isTrue();
  }

  public void testMoveDirToParent() throws IOException {
    createDmo(asList("/a/b/c/1.txt", "/a/b/c/2.jpg"));
    val folder = new DMOFile("/a/b/c");
    folder.rename("/a");
    assertThat(new DMOFile("/a/b/c").exists()).isFalse();
    assertThat(new DMOFile("/a/c").exists()).isTrue();
    assertThat(new DMOFile("/a/b/c/1.txt").exists()).isFalse();
    assertThat(new DMOFile("/a/c/1.txt").exists()).isTrue();
    assertThat(new DMOFile("/a/c/2.jpg").exists()).isTrue();
  }

  public void testCachedSize() throws IOException {
    val dmoFile = createDmo("testCachedSize", "12345");
    assertThat(dmoFile.getSize()).isEqualTo(5);
    try (val os = new DMOOutputStream(new DMOFile("testCachedSize"), true)) {
      os.write("6789".getBytes());
    }
    assertThat(dmoFile.getSize()).isEqualTo(5);
    dmoFile.close(dmoFile.open(false).getRc());
    assertThat(dmoFile.getSize()).isEqualTo(9);
  }

  public void testGetPath() throws IOException {
    assertThat(new DMOFile("/").getPath("a")).isEqualTo("/a");
    assertThat(new DMOFile("/b").getPath("a")).isEqualTo("/b/a");
    assertThat(new DMOFile("/b/").getPath("a")).isEqualTo("/b/a");
  }

  public void testMmap() throws IOException {
    val dmoFile = createDmo("testMmap", "1234567890");
    val buffer = dmoFile.mmap(4);
    assertThat(bufferToString(buffer)).isEqualTo("1234");
  }

  private String bufferToString(ByteBuffer buffer) {
    byte[] bytes;
    if (buffer.hasArray()) {
      bytes = buffer.array();
    } else {
      bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
    }
    return new String(bytes, Charset.forName("UTF-8"));
  }
}

package com.memverge.mvfs.dmo;

import static com.memverge.mvfs.TestGroups.IT;
import static com.memverge.mvfs.TestGroups.UT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.memverge.mvfs.error.MVFSException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import lombok.val;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {UT, IT})
public class DMOTmpFileTest {

  private static final String content = "hello";

  @BeforeMethod
  private void beforeMethod() {
    afterMethod();
  }

  @AfterMethod
  private void afterMethod() {
    DMOFile.safeReset();
  }

  private DMOTmpFile getTmpFile(String commitTargetId) throws IOException {
    val dmoFile = new DMOFile(commitTargetId);
    val tmpFile = DMOTmpFile.make(dmoFile);
    try (val os = new OutputStreamWriter(new DMOOutputStream(tmpFile))) {
      os.write(content);
    }
    return tmpFile;
  }

  public void testTmpFile() throws IOException {
    val tmpFile = DMOTmpFile.make();
    assertThat(tmpFile.exists()).isTrue();
    assertThat(tmpFile.getDmoId()).contains("tmp");
    tmpFile.delete();
    assertThat(tmpFile.exists()).isFalse();
  }

  public void testCommitChange() throws IOException {
    val tmpFile = getTmpFile("commitChange");
    val dmoFile = tmpFile.getCommitTarget();
    assertThat(dmoFile.exists()).isFalse();

    tmpFile.commit();
    assertThat(dmoFile.exists()).isTrue();
    assertThat(tmpFile.exists()).isFalse();

    assertThat(new String(dmoFile.read())).isEqualTo(content);
  }

  public void testCommitOverwriteExistingTarget() throws IOException {
    val targetName = "commitOverwrite";
    DMOFile targetFile = new DMOFile(targetName);
    targetFile.create();
    try (val os = new OutputStreamWriter(new DMOOutputStream(targetFile))) {
      os.write("123");
    }
    assertThat(targetFile.getSize()).isEqualTo(3);

    val tmpFile = getTmpFile(targetName);
    tmpFile.commit();
    targetFile = new DMOFile(targetName);
    assertThat(targetFile.getSize()).isEqualTo(content.length());
    assertThat(new String(targetFile.read())).isEqualTo(content);
  }

  public void testNoCommitTarget() throws IOException {
    val tmpFile = DMOTmpFile.make();
    assertThatExceptionOfType(MVFSException.class).isThrownBy(tmpFile::commit);
  }

  public void testDoubleCommit() throws IOException {
    val tmpFile = getTmpFile("doubleCommit");
    tmpFile.commit();
    assertThatExceptionOfType(MVFSException.class).isThrownBy(tmpFile::commit);
  }

  public void testRecall() throws IOException {
    val tmpFile = getTmpFile("recall");
    tmpFile.recall();
    assertThat(tmpFile.exists()).isFalse();
  }

  public void testDoubleRecall() throws IOException {
    val tmpFile = getTmpFile("recall");
    tmpFile.recall();
    tmpFile.recall();
    assertThat(tmpFile.exists()).isFalse();
  }

  public void testListAllTmpFiles() throws IOException {
    assertThat(DMOTmpFile.listAll()).isEmpty();
    val tmpFile = DMOTmpFile.make();
    assertThat(DMOTmpFile.listAll()).containsExactly(tmpFile.getName());
  }

  public void testSwap() throws IOException {
    val other = getTmpFile("targetOfOther");
    val target = new DMOFile("target");
    val tmpFile = DMOTmpFile.make(target);
    assertThat(tmpFile.exists()).isTrue();
    assertThat(tmpFile.getCommitTarget().exists()).isFalse();
    tmpFile.swap(other);
    assertThat(tmpFile.exists()).isTrue();
    assertThat(other.exists()).isFalse();
    tmpFile.commit();
    assertThat(tmpFile.exists()).isFalse();
    assertThat(other.exists()).isFalse();
    assertThat(target.exists()).isTrue();
    assertThat(new DMOFile("targetOfOthers").exists()).isFalse();
    assertThat(new String(target.read())).isEqualTo(content);
  }

  public void testSwapCommittedFile() throws IOException {
    val other = getTmpFile("targetOfOther");
    other.commit();
    val tmpFile = DMOTmpFile.make(new DMOFile("target"));
    assertThatExceptionOfType(MVFSException.class)
        .isThrownBy(() -> tmpFile.swap(other));
  }

  public void testRemoveOtherFileAfterSwap() throws IOException {
    val other = getTmpFile("targetOfOther");
    val target = new DMOFile("target");
    val tmpFile = DMOTmpFile.make(target);
    tmpFile.swap(other);
    other.recall();
    assertThat(tmpFile.exists()).isTrue();
    assertThat(other.exists()).isFalse();
    tmpFile.commit();
    assertThat(tmpFile.exists()).isFalse();
    assertThat(target.exists()).isTrue();
    assertThat(new String(target.read())).isEqualTo(content);
  }
}

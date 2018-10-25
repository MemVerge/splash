package com.memverge.mvfs.utils;

import static com.memverge.mvfs.TestGroups.UT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.nio.file.Paths;
import org.testng.annotations.Test;

@Test(groups = UT)
public class TmpFileFolderTest {

  private TmpFileFolder folder = new TmpFileFolder("ut");

  public void testRelativePath() {
    String path = Paths.get(folder.getTmpPath(), "a", "b.txt").toString();
    assertThat(folder.getRelativePath(path)).isEqualTo("/a/b.txt");
  }
}

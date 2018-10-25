package com.memverge.mvfs.hdfs;

import static com.memverge.mvfs.TestGroups.IT;
import static com.memverge.mvfs.TestGroups.UT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.internal.collections.Ints.asList;

import com.memverge.mvfs.dmo.DMOFile;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.zip.CRC32;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = {UT, IT})
public class HdfsDataVerificationTest {

  private DMOFileSystem dmoFs;
  private HdfsTestUtil testUtil;

  @BeforeMethod
  private void beforeMethod() throws URISyntaxException, IOException {
    dmoFs = HdfsTestUtil.createDmoFs();
    testUtil = new HdfsTestUtil(dmoFs);
  }

  @AfterMethod
  private void afterMethod() {
    DMOFile.safeReset();
  }

  @DataProvider(name = "hdfsFileSizeProvider")
  public Object[] hdfsFileSizeProvider() {
    return asList(
        1,
        10,
        100,
        10000,
        50000).toArray();
  }

  @Test(dataProvider = "hdfsFileSizeProvider")
  public void testHdfsWriteReadFileOfSize(int size) throws IOException {
    val name = String.format("hdfsReadWrite_%d", size);
    verifyRandomFile(name, size);
  }

  private void verifyRandomFile(String name, int fileSize) throws IOException {
    val fileInfo = testUtil.genRandomDmoFile(name, fileSize);
    try (val is = dmoFs.open(new Path(fileInfo.getName()))) {
      val size = (int) fileInfo.getSize();
      val buf = new byte[size];
      val crc = new CRC32();
      val bytesRead = is.read(buf, 0, size);
      crc.update(buf, 0, size);
      assertThat(bytesRead).isEqualTo(size);
      assertThat(crc.getValue()).isEqualTo(fileInfo.getCrc());
    }
  }
}

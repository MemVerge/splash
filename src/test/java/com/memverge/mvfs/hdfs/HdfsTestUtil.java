package com.memverge.mvfs.hdfs;

import com.memverge.mvfs.utils.FileInfo;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.zip.CRC32;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

@Slf4j
class HdfsTestUtil {

  static DMOFileSystem createDmoFs() throws URISyntaxException, IOException {
    val dmoFs = new DMOFileSystem();
    dmoFs.initialize(new URI("dmo://0/"), new Configuration());
    return dmoFs;
  }

  private DMOFileSystem dmoFs;

  HdfsTestUtil(DMOFileSystem dmoFs) {
    this.dmoFs = dmoFs;
  }

  private Path getPath(String name) {
    return new Path(name);
  }

  FileInfo genRandomDmoFile(String name, long size) throws IOException {
    val crcMaker = new CRC32();
    int written = 0;
    try (val os = dmoFs.create(getPath(name))) {
      val rand = new Random();
      val iSize = (int) size;
      val buf = new byte[iSize];
      rand.nextBytes(buf);
      os.write(buf, written, iSize);
      crcMaker.update(buf, 0, iSize);
    } catch (IOException e) {
      log.error("write random file {} failed.", name, e);
      throw e;
    }
    return new FileInfo(name, crcMaker.getValue(), size);
  }
}

/*
 * Copyright (C) 2019 MemVerge Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.memverge.splash;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import lombok.val;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = {"UnitTest", "IntegrationTest"})
public class SplashBufferedOutputStreamTest {

  private StorageFactory factory = StorageFactoryHolder.getFactory();

  @AfterClass
  private void afterClass() {
    factory.reset();
  }

  private TmpShuffleFile getTestFile() throws IOException {
    val spill = factory.makeSpillFile();
    try (val os = spill.makeOutputStream()) {
      os.write("1234567890".getBytes());
    }
    return spill;
  }

  public void testGetBuffer() throws IOException {
    val file = getTestFile();
    val bos = file.makeBufferedOutputStream();
    assertThat(bos.getBuffer()).isNotNull();
  }

  public void testGetBufferSize() throws IOException {
    val file = getTestFile();
    val bos = new SplashBufferedOutputStream(file.makeOutputStream());
    assertThat(bos.getBufferSize()).isEqualTo(factory.getFileBufferSize());
  }

  public void testWriteMultipleTimes() throws IOException {
    val file = factory.makeSpillFile();
    try (val os = file.makeBufferedOutputStream(8)) {
      os.write("--Hello".getBytes(), 2);
      os.write("= World!".getBytes(), 1);
      os.write(" From buffered output stream".getBytes(), 0);
    }
    try (val is = file.makeBufferedInputStream()) {
      val bytes = new byte[(int) file.getSize()];
      assertThat(is.read(bytes)).isEqualTo(bytes.length);
      assertThat(new String(bytes))
          .isEqualTo("Hello World! From buffered output stream");
    }
  }

  public void testCloseAutoFlushTheBuffer() throws IOException {
    val animal = "A cat";
    val file = factory.makeSpillFile();
    try (val os = file.makeBufferedOutputStream(100)) {
      os.write(animal.getBytes());
    }
    try (val is = file.makeBufferedInputStream()) {
      val bytes = new byte[animal.length()];
      assertThat(is.read(bytes)).isEqualTo(animal.length());
      assertThat(new String(bytes)).isEqualTo(animal);
    }
  }
}

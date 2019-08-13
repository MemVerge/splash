/*
 * Copyright (C) 2018 MemVerge Inc.
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
public class ShuffleFileTest {

  private StorageFactory factory = StorageFactoryHolder.getFactory();

  @AfterClass
  private void afterClass() {
    factory.reset();
  }

  private TmpShuffleFile getTestFile() throws IOException {
    val spill = factory.makeSpillFile();
    try (val os = spill.makeOutputStream()) {
      os.write("123456".getBytes());
    }
    return spill;
  }

  public void testMakeBufferedInputStreamWithin() throws IOException {
    val spill = getTestFile();

    try (val is = spill.makeBufferedInputStreamWithin(2, 4)) {
      val buffer = new byte[10];
      assertThat(is.read(buffer)).isEqualTo(2);
      assertThat(new String(buffer)).startsWith("34");
    }
  }

  public void testMakeBufferedInputStreamFrom() throws IOException {
    val spill = getTestFile();

    try (val is = spill.makeBufferedInputStreamWithin(3, -1)) {
      val buffer = new byte[10];
      assertThat(is.read(buffer)).isEqualTo(3);
      assertThat(new String(buffer)).startsWith("456");
    }
  }
}

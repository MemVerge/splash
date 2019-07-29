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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import lombok.val;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = {"UnitTest", "IntegrationTest"})
public class ManualCloseOutputStreamTest {

  private StorageFactory factory = StorageFactoryHolder.getFactory();

  @AfterClass
  private void afterClass() {
    factory.reset();
  }

  public void testManualClose() throws IOException {
    val file = factory.makeSpillFile();
    val size = 6;
    val os = file.makeBufferedManualCloseOutputStream();
    os.write("123");
    os.close();
    os.write("456");
    os.manualClose();

    try (val is = file.makeInputStream()) {
      val buffer = new byte[size];
      assertThat(is.read(buffer)).isEqualTo(size);
      assertThat(new String(buffer)).isEqualTo("123456");
    }
  }

  public void testWriteAfterClose() throws IOException {
    val file = factory.makeSpillFile();
    val os = file.makeBufferedManualCloseOutputStream();
    os.write("123");
    os.manualClose();

    assertThatExceptionOfType(IOException.class).isThrownBy(() -> {
      os.write("456");
      os.manualClose();
    });
  }

  public void testGetCount() throws IOException {
    val file = factory.makeSpillFile();
    try (val os = file.makeBufferedManualCloseOutputStream()) {
      os.write("123");
      assertThat(os.getCount()).isEqualTo(3);

      os.write("456");
      assertThat(os.getCount()).isEqualTo(6);
      os.manualClose();
    }
  }

  @SuppressWarnings("deprecation")
  public void testMetrics() throws IOException {
    val metrics = new ShuffleWriteMetrics();
    val file = factory.makeSpillFile();

    try (val os = file.makeBufferedManualCloseOutputStream(metrics)) {
      assertThat(metrics.shuffleWriteTime()).isEqualTo(0);

      for (int i = 0; i < 100; i++) {
        os.write(i);
      }

      assertThat(metrics.shuffleWriteTime()).isGreaterThan(0L);
      os.manualClose();
    }
  }
}

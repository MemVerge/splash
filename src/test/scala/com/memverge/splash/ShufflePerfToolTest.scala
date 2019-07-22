package com.memverge.splash

import java.time.Duration

import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.{BeforeMethod, Test}

@Test(groups = Array("UnitTest", "IntegrationTest"))
class ShufflePerfToolTest {
  @BeforeMethod
  private def beforeMethod(): Unit = {
    StorageFactoryHolder.getFactory.reset()
  }

  def testUsage(): Unit = {
    val ret = ShufflePerfTool.parse(Array("-h"))
    assertThat(ret.isLeft).isTrue
  }

  def testWriteReadShuffleWithDefaultConfig(): Unit = {
    val start = System.nanoTime()
    ShufflePerfTool.execute(Array("-b", "1024"))
    val duration = Duration.ofNanos(System.nanoTime() - start)
    assertThat(duration.toMillis).isLessThan(2000)
  }

  def testInvalidIntParameter(): Unit = {
    val ret = ShufflePerfTool.parse(Array("-b", "block"))
    assertThat(ret.isLeft).isTrue
    ret.left.map(msg => assertThat(msg).contains("invalid integer"))
  }
}

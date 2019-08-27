package com.memverge.splash

import java.time.Duration

import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.{AfterMethod, BeforeMethod, Test}

@Test(groups = Array("UnitTest", "IntegrationTest"))
class ShufflePerfToolTest {
  @BeforeMethod
  private def beforeMethod(): Unit = afterMethod()

  @AfterMethod
  private def afterMethod(): Unit = StorageFactoryHolder.getFactory.reset()

  def testUsage(): Unit = {
    val ret = ShufflePerfTool.parse(Array("-h"))
    assertThat(ret.isLeft).isTrue
  }

  def testWriteReadShuffleWithDefaultConfig(): Unit = {
    val start1 = System.nanoTime()
    ShufflePerfTool.execute(Array("-b", "1024"))
    val duration1 = Duration.ofNanos(System.nanoTime() - start1)
    assertThat(duration1.toMillis).isLessThan(2000)

    val start2 = System.nanoTime()
    ShufflePerfTool.execute(Array("-b", "1024", "-ro"))
    val duration2 = Duration.ofNanos(System.nanoTime() - start2)
    assertThat(duration2.toMillis).isLessThan(duration1.toMillis)
  }

  def testInvalidIntParameter(): Unit = {
    val ret = ShufflePerfTool.parse(Array("-b", "block"))
    assertThat(ret.isLeft).isTrue
    ret.left.map(msg => assertThat(msg).contains("invalid integer"))
  }

  def testToSizeStr(): Unit = {
    val scale = ShufflePerfTool.toSizeStrDouble(1000L)(_)
    assertThat(scale(987)).isEqualTo("987.0")
    assertThat(scale(512 * 1e3)).isEqualTo("512.00K")
    assertThat(scale(1010 * 1e3)).isEqualTo("1.01M")
    assertThat(scale(1237 * 1e6)).isEqualTo("1.24G")
    assertThat(scale(5678 * 1e9)).isEqualTo("5.68T")
  }
}

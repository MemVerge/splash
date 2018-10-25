/*
 * Copyright (c) 2018  MemVerge Inc.
 */
package org.apache.spark.shuffle.memverge

import com.memverge.mvfs.connection.MVFSConnectOptions
import com.memverge.mvfs.dmo.DMOFile
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{JobConf, KeyValueTextInputFormat, TextOutputFormat}
import org.apache.spark.SparkContext
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.{AfterClass, BeforeClass, Test}

@Test(groups = Array("UnitTest", "IntegrationTest"))
class DMOFileSystemScalaTest {
  private var sc: SparkContext = _

  @BeforeClass
  def beforeClass(): Unit = {
    sc = TestUtil.newSparkContext(TestUtil.newSparkConf(4))
  }

  @AfterClass
  def afterClass(): Unit = {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    DMOFile.safeReset()
  }

  def testPairSaveAsHadoopFile(): Unit = {
    val arr = Array(("A", "2"), ("A", "1"), ("B", "6"), ("B", "3"))
    val rdd = sc.makeRDD(arr)
    val filename = toUrl("/saveAsHadoopFile/strIntPair")
    rdd.saveAsHadoopFile(filename,
      classOf[Text],
      classOf[Text],
      classOf[TextOutputFormat[Text, Text]])

    val fromFile = sc.hadoopFile[Text, Text, KeyValueTextInputFormat](filename)
    val arrFromFile = fromFile.map { case (x, y) => (x.toString, y.toString) }
    assertThat(arrFromFile.collect().toSet) isEqualTo arr.toSet
  }

  def testPairSaveAsHadoopDataSet(): Unit = {
    val rdd = sc.makeRDD(Array(("A", "2"), ("A", "1"), ("B", "6"), ("B", "3")))
    val jobConf = new JobConf()
    jobConf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])
    jobConf.setOutputKeyClass(classOf[Text])
    jobConf.setOutputValueClass(classOf[Text])
    jobConf.set("mapred.output.dir", toUrl("/saveAsHadoopDataSet/"))
    rdd.saveAsHadoopDataset(jobConf)
    assertThat(rdd.count()) isEqualTo 4
  }

  private def toUrl(path: String) = {
    val socket = MVFSConnectOptions.defaultSocket
    String.format("dmo://%s%s", socket, path)
  }
}

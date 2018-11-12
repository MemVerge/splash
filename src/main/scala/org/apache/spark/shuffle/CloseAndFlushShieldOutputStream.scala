package org.apache.spark.shuffle

import java.io.OutputStream

import org.apache.commons.io.output.CloseShieldOutputStream

case class CloseAndFlushShieldOutputStream(outputStream: OutputStream)
    extends CloseShieldOutputStream(outputStream) {
  override def flush(): Unit = {
    // Prevent actual flush, do nothing
  }
}

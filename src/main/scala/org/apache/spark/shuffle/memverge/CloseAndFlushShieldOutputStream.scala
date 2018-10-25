/*
 * Copyright (c) 2018  MemVerge Inc.
 */

package org.apache.spark.shuffle.memverge

import java.io.OutputStream

import org.apache.commons.io.output.CloseShieldOutputStream


case class CloseAndFlushShieldOutputStream(outputStream: OutputStream)
    extends CloseShieldOutputStream(outputStream) {
  override def flush(): Unit = {
    // Do nothing
  }
}

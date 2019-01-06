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
package org.apache.spark.shuffle

import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.testng.{ITestResult, TestListenerAdapter}

class LogListener extends TestListenerAdapter with Logging {
  override def onTestStart(tr: ITestResult): Unit = {
    super.onTestStart(tr)
    logInfo(s"--- ${tr.getName}${getParams(tr)} --- test start.")
  }

  override def onTestFailure(tr: ITestResult): Unit = {
    logError(s"--- ${tr.getName}${getParams(tr)} --- failed, took ${getSeconds(tr)}s.")
    val params = tr.getParameters
    if (params.nonEmpty) {
      logError(s"test parameters: $params.")
    }
    logError("detail:", tr.getThrowable)
  }

  override def onTestSkipped(tr: ITestResult): Unit = {
    logWarning(s"--- ${tr.getName}${getParams(tr)} --- skipped, took ${getSeconds(tr)}s.")
  }

  override def onTestSuccess(tr: ITestResult): Unit = {
    logInfo(s"--- ${tr.getName}${getParams(tr)} --- passed, took ${getSeconds(tr)}s.")
  }

  private def getParams(tr: ITestResult): String = {
    val params = tr.getParameters
    if (params.nonEmpty) {
      s" [${StringUtils.join(params, ", ")}]"
    } else {
      ""
    }
  }

  private def getSeconds(tr: ITestResult) = (tr.getEndMillis - tr.getStartMillis).toDouble / 1000
}

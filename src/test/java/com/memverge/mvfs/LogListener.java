package com.memverge.mvfs;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

@Slf4j
public class LogListener extends TestListenerAdapter {

  @Override
  public void onTestStart(ITestResult tr) {
    super.onTestStart(tr);
    log.info("--- {}{} --- test start.", tr.getName(), getParams(tr));
  }

  @Override
  public void onTestFailure(ITestResult tr) {
    log.error("--- {}{} --- failed, took {}s.",
        tr.getName(), getParams(tr), getSeconds(tr));
    val params = tr.getParameters();
    if (params.length > 0) {
      log.error("test parameters: {}.", params);
    }
    log.error("detail:", tr.getThrowable());
  }

  @Override
  public void onTestSkipped(ITestResult tr) {
    log.warn("--- {}{} --- skipped, took {}s.",
        tr.getName(), getParams(tr), getSeconds(tr));
  }

  @Override
  public void onTestSuccess(ITestResult tr) {
    log.info("--- {}{} --- passed, took {}s.",
        tr.getName(), getParams(tr), getSeconds(tr));
  }

  private String getParams(ITestResult tr) {
    val params = tr.getParameters();
    String ret = "";
    if (params.length > 0) {
      ret = String.format(" [%s]", StringUtils.join(params, ", "));
    }
    return ret;
  }

  private double getSeconds(ITestResult tr) {
    return (double) (tr.getEndMillis() - tr.getStartMillis()) / 1000;
  }
}

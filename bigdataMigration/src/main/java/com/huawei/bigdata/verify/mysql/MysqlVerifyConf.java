package com.huawei.bigdata.verify.mysql;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

public class MysqlVerifyConf {

  @JsonProperty("report.dir")
  private String reportDir;

  @JsonProperty("mysql.confs")
  private List<VerifyConf> confs;

  private int threads;

  public int getThreads() {
    return threads;
  }

  public String getReportDir() {
    return reportDir;
  }

  public List<VerifyConf> getConfs() {
    return confs;
  }

  public static class VerifyConf {

    private String name;
    private MysqlConf source;
    private MysqlConf target;

    public MysqlConf getSource() {
      return source;
    }

    public MysqlConf getTarget() {
      return target;
    }
  }

  private static class MysqlConf {

    private String url;
    private String user;
    private String password;

    public String getUrl() {
      return url;
    }

    public String getUser() {
      return user;
    }

    public String getPassword() {
      return password;
    }
  }

}

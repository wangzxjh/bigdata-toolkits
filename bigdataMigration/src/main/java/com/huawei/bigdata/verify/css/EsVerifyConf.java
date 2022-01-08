package com.huawei.bigdata.verify.css;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class EsVerifyConf {

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
    private List<String> excludeDatabase;
    private List<String> includeDatabase;
    private List<String> includeTables;

    private EsConf source;
    private EsConf target;

    public String getName() {
      return name;
    }

    public List<String> getIncludeTables() {
      return includeTables;
    }

    public List<String> getExcludeDatabase() {
      return excludeDatabase;
    }

    public List<String> getIncludeDatabase() {
      return includeDatabase;
    }

    public EsConf getSource() {
      return source;
    }

    public EsConf getTarget() {
      return target;
    }
  }

  public static class EsConf {

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

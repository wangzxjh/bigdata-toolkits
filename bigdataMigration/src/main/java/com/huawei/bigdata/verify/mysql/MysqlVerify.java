package com.huawei.bigdata.verify.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.huawei.bigdata.cmmon.DateUtil;
import com.huawei.bigdata.verify.mysql.MysqlVerifyConf.VerifyConf;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlVerify {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlVerify.class);

  public static void main(String[] args)
      throws ParseException, ClassNotFoundException, SQLException, IOException {

    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    MysqlVerifyConf conf = mapper
        .readValue(MysqlVerify.class.getResourceAsStream("/mysqlVerify.yml"),
            MysqlVerifyConf.class);

    LOG.info("mysql verify conf:\n {}", mapper.writeValueAsString(conf));

    Class.forName("com.mysql.cj.jdbc.Driver");

    ExecutorService executorService = Executors.newFixedThreadPool(conf.getThreads(),
        r -> new Thread(r, "thread pool-" + r.hashCode()));

    String reportDir = conf.getReportDir() + "/" + DateUtil.currentDate();

    for (VerifyConf confConf : conf.getConfs()) {
      executorService.submit(() -> {

      });
    }

//    Connection sourceMysqlConn = DriverManager
//        .getConnection(sourceMysql, sourceMysqlUser, sourceMysqlPasswd);
//
//    Connection targetMysqlConn = DriverManager
//        .getConnection(sourceMysql, sourceMysqlUser, sourceMysqlPasswd);

  }

  public static class MysqlInstanceVerify implements Runnable {

    @Override
    public void run() {

    }
  }
}

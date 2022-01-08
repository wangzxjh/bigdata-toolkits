package com.huawei.bigdata.verify.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.huawei.bigdata.cmmon.DateUtil;
import com.huawei.bigdata.verify.mysql.MysqlInstanceVerify.Metric;
import com.huawei.bigdata.verify.mysql.MysqlVerifyConf.MysqlConf;
import com.huawei.bigdata.verify.mysql.MysqlVerifyConf.VerifyConf;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlVerify {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlVerify.class);

  public static void main(String[] args) throws IOException, InterruptedException {

    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    MysqlVerifyConf conf = null;
    try {
      conf = mapper
          .readValue(MysqlVerify.class.getResourceAsStream("/mysqlVerify.yml"),
              MysqlVerifyConf.class);
      LOG.info("mysql verify conf:\n {}", mapper.writeValueAsString(conf));
    } catch (IOException e) {
      LOG.error("read mysql conf failed", e);
      return;
    }

    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      LOG.error("cannot find class com.mysql.cj.jdbc.Driver", e);
      return;
    }

    ExecutorService executorService = Executors.newFixedThreadPool(conf.getThreads(),
        r -> new Thread(r, "thread pool-" + r.hashCode()));

    String outputFile = reportOuputFile(conf);
    LOG.info("output file:{}", outputFile);
    try (OutputStream outputStream = new FileOutputStream(outputFile)) {
      outputStream.write((Metric.header() + "\n").getBytes(StandardCharsets.UTF_8));

      List<Future<Metric>> futures = new ArrayList<>();
      for (VerifyConf verifyConf : conf.getConfs()) {
        LOG.info("proccess conf:{}", verifyConf.getName());
        MysqlConf source = verifyConf.getSource();

        try (Connection sourceMysqlConn = DriverManager.getConnection(source.getUrl(),
            source.getUser(), source.getPassword());
            Statement sourceSt = sourceMysqlConn.createStatement();
            ResultSet rs = sourceSt.executeQuery("show databases");) {

          while (rs.next()) {
            String database = rs.getString(1).toLowerCase(Locale.ROOT);
            if (skipDatabase(verifyConf, database)) {
              LOG.info("skip database:{}", database);
              continue;
            }

            try (Statement statement = sourceMysqlConn.createStatement();
                ResultSet resultSet = statement
                    .executeQuery(String.format("show tables from `%s`", database));) {
              while (resultSet.next()) {
                String table = resultSet.getString(1).toLowerCase(Locale.ROOT);
                if (skipTable(verifyConf, database, table)) {
                  LOG.info("skip database:{} table:{}", database, table);
                  continue;
                }
                futures.add(
                    executorService.submit(new MysqlInstanceVerify(verifyConf, database, table)));
              }
            }
          }
        } catch (Exception e) {
          LOG.error("mysql verify error", e);
        }

        for (Future<Metric> future : futures) {
          try {
            Metric metric = future.get();
            outputStream.write((metric.toString() + "\n").getBytes(StandardCharsets.UTF_8));
          } catch (Exception e) {
            LOG.error("failed compute metric", e);
          }
        }
        futures.clear();
      }
    }

    executorService.shutdown();
    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
  }

  private static boolean skipTable(VerifyConf verifyConf, String database, String table) {
    return verifyConf.getIncludeTables() != null && !verifyConf.getIncludeTables().contains(
        String.format("%s.%s", database, table));
  }

  private static boolean skipDatabase(VerifyConf verifyConf, String database) {
    return verifyConf.getExcludeDatabase().contains(database) ||
        verifyConf.getIncludeDatabase() != null
            && !verifyConf.getIncludeDatabase().isEmpty()
            && !verifyConf.getIncludeDatabase().contains(database);
  }

  private static String reportOuputFile(MysqlVerifyConf conf) {
    String reportDir =
        MysqlVerifyConf.class.getResource("/").getPath() + "/" + conf.getReportDir() + "/"
            + DateUtil.currentDate();
    File file = new File(reportDir);
    if (!file.exists()) {
      file.mkdirs();
    }
    return reportDir + "/report.csv";
  }
}
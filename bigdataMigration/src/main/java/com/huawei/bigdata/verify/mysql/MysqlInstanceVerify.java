package com.huawei.bigdata.verify.mysql;

import com.huawei.bigdata.verify.mysql.MysqlInstanceVerify.Metric;
import com.huawei.bigdata.verify.mysql.MysqlVerifyConf.MysqlConf;
import com.huawei.bigdata.verify.mysql.MysqlVerifyConf.VerifyConf;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlInstanceVerify implements Callable<Metric> {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlInstanceVerify.class);

  private final VerifyConf verifyConf;
  private final String database;
  private final String table;

  public MysqlInstanceVerify(VerifyConf verifyConf, String database, String table) {
    this.verifyConf = verifyConf;
    this.database = database;
    this.table = table;
  }


  @Override
  public Metric call() throws Exception {
    LOG.info("process {}.{}.{}", verifyConf.getName(), database, table);
    MysqlConf source = verifyConf.getSource();
    MysqlConf target = verifyConf.getTarget();

    try (Connection sourceMysqlConn = DriverManager.getConnection(source.getUrl(),
        source.getUser(), source.getPassword());
        Connection targetMysqlConn = DriverManager.getConnection(target.getUrl(),
            target.getUser(), target.getPassword())) {
      Metric metric = new Metric();
      metric.instanceName = verifyConf.getName();
      metric.database = database;
      metric.table = table;
      metric.targetTableExists = tableExists(targetMysqlConn, database, table);
      if (!metric.targetTableExists) {
        return metric;
      }

      String intColumn = randomFindIntColumn(sourceMysqlConn, database, table);

      String sql;
      if (intColumn != null) {
        sql = String.format("select count(1),sum(%s) from `%s`.`%s`", intColumn, database, table);
      } else {
        sql = String.format("select count(1) from `%s`.`%s`", database, table);
      }

      long sourceCount = 0;
      long sourceIntSum = 0;
      try (Statement sourceSt = sourceMysqlConn.createStatement();
          ResultSet sourceRs = sourceSt.executeQuery(sql)) {
        if (sourceRs.next()) {
          sourceCount = sourceRs.getLong(1);
          if (intColumn != null) {
            sourceIntSum = sourceRs.getLong(2);
          }

        }
      }

      long targetCount = 0;
      long targetIntSum = 0;
      try (Statement targetSt = targetMysqlConn.createStatement();
          ResultSet targetRs = targetSt.executeQuery(sql)) {
        if (targetRs.next()) {
          targetCount = targetRs.getLong(1);
          if (intColumn != null) {
            targetIntSum = targetRs.getLong(2);
          }
        }
      }

      metric.rowCount = Pair.of(sourceCount, targetCount);

      if (intColumn != null) {
        metric.sum = Pair.of(intColumn, Pair.of(sourceIntSum, targetIntSum));
      }
      return metric;
    }
  }

  private String randomFindIntColumn(
      Connection source, String database, String table) throws SQLException {
    List<String> intColumns = new ArrayList<>();
    try (Statement statement = source.createStatement();
        ResultSet rs = statement
            .executeQuery(String.format("describe `%s`.`%s`", database, table))) {
      while (rs.next()) {
        if (rs.getString(2).toLowerCase(Locale.ROOT).contains("int")) {
          intColumns.add(rs.getString(1));
        }
      }

    }
    if (intColumns.isEmpty()) {
      return null;
    }

    Random random = new Random();
    return intColumns.get(random.nextInt(intColumns.size()));
  }

  private boolean tableExists(Connection targetMysqlConn, String database, String table)
      throws SQLException {
    try (Statement sourceSt = targetMysqlConn.createStatement();
        ResultSet rs = sourceSt.executeQuery(String.format("show tables from `%s`", database))) {
      while (rs.next()) {
        if (table.equals(rs.getString(1).toLowerCase(Locale.ROOT))) {
          return true;
        }
      }
    }
    return false;
  }


  static class Metric {

    String instanceName;
    String database;
    String table;
    // target mysql table exists
    boolean targetTableExists;
    Pair<Long, Long> rowCount;
    Pair<String, Pair<Long, Long>> sum;


    @Override
    public String toString() {
      StringBuffer stringBuffer = new StringBuffer();
      stringBuffer.append(instanceName)
          .append(",").append(database)
          .append(",").append(table)
          .append(",").append(targetTableExists);

      if (rowCount != null) {
        double diff;
        if (rowCount.getRight() != 0) {
          diff = (rowCount.getLeft() - rowCount.getRight()) * 1.0 / rowCount.getRight();
        } else if (rowCount.getRight().equals(rowCount.getLeft())) {
          diff = 0;
        } else {
          diff = Double.MAX_VALUE;
        }
        stringBuffer
            .append(",").append(rowCount.getLeft())
            .append(",").append(rowCount.getRight())
            .append(",").append(diff);
      }
      if (sum != null) {
        stringBuffer.append(",").append(sum.getLeft())
            .append(",").append(sum.getRight().getLeft())
            .append(",").append(sum.getRight().getLeft());

        double diff;
        if (sum.getRight().getRight() != 0) {
          diff = (sum.getRight().getLeft() - sum.getRight().getRight()) * 1.0 / sum.getRight()
              .getRight();
        } else if (sum.getRight().getRight().equals(sum.getRight().getLeft())) {
          diff = 0;
        } else {
          diff = Double.MAX_VALUE;
        }
        stringBuffer.append(",").append(diff);
      }

      return stringBuffer.toString();
    }

    public static String header() {
      return "instance,database,table, targetTableExists,source rowcount, target rowcount, rowcount diff, intcolumn, source sum, target sum, sum diff";
    }
  }
}

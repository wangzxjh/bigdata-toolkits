package com.huawei.bigdata.data;

import com.huawei.bigdata.cmmon.DateUtil;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Objects;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 */
public class BatchDelete {

  public static final Logger LOG = LoggerFactory.getLogger(BatchDelete.class);

  public static final String TO_DELETE_SOURCE_DIR = "toDeleteSourceDir";
  public static final String THREADS = "threads";
  public static final String EXCLUDE_DATABASE = "excludeDatabase";
  public static final String EXCLUDE_TABLES = "excludeTables";
  public static final String PATH_PREFIX = "pathPrefix";
  public static final String BEGIN_DATE = "beginDate";
  public static final String END_DATE = "endDate";
  public static final String DRY_RUN = "dryRun";

  public static void main(String[] args)
      throws ParseException, java.text.ParseException, InterruptedException {
    Options options = new Options();
    options.addOption(TO_DELETE_SOURCE_DIR, true, "删除文件数据的根目录");
    options.addOption(THREADS, true, "线程数");
    options.addOption(EXCLUDE_DATABASE, true, "不删除的数据库");
    options.addOption(EXCLUDE_TABLES, true, "不删除的表");
    options.addOption(PATH_PREFIX, true, "路径前缀");
    options.addOption(BEGIN_DATE, true, "起始时间");
    options.addOption(END_DATE, true, "终止时间");
    options.addOption(DRY_RUN, true, "是否空跑");
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    String sourceDir;
    if (cmd.hasOption(TO_DELETE_SOURCE_DIR)) {
      sourceDir = cmd.getOptionValue(TO_DELETE_SOURCE_DIR);
    } else {
      throw new RuntimeException("need " + TO_DELETE_SOURCE_DIR);
    }
    if (StringUtils.isEmpty(sourceDir)) {
      throw new RuntimeException(TO_DELETE_SOURCE_DIR + " value is empty");
    }
    LOG.info("{}={}", TO_DELETE_SOURCE_DIR, sourceDir);

    int threads = Integer.parseInt(cmd.getOptionValue(THREADS, "10"));
    LOG.info("{}={}", THREADS, threads);

    Set<String> excludeDatabases = new HashSet<>(
        Arrays.asList(cmd.getOptionValue(EXCLUDE_DATABASE, "").split(",")));
    LOG.info("{} {}", EXCLUDE_DATABASE, excludeDatabases);

    Set<String> excludeTables = new HashSet<>(
        Arrays.asList(cmd.getOptionValue(EXCLUDE_TABLES, "").split(",")));
    LOG.info("{} {}", EXCLUDE_TABLES, excludeTables);

    String pathPrefix = cmd.getOptionValue(PATH_PREFIX, "");
    LOG.info("{} {}", PATH_PREFIX, pathPrefix);

    String begin = cmd.getOptionValue(BEGIN_DATE, "");
    Date beginDate = StringUtils.isEmpty(begin) ? null : DateUtil.parseDate(begin);
    LOG.info("{} {}", BEGIN_DATE, beginDate);

    String end = cmd.getOptionValue(END_DATE, "");
    Date endDate = StringUtils.isEmpty(end) ? null : DateUtil.parseDate(end);
    LOG.info("{} {}", END_DATE, endDate);

    boolean dryRun = Boolean.parseBoolean(cmd.getOptionValue(DRY_RUN, "true"));
    LOG.info("{} {}", DRY_RUN, dryRun);

    Configuration conf = new Configuration();

    ExecutorService pool = Executors.newFixedThreadPool(threads);

    File root = new File(sourceDir);

    findFile(root, (file) -> {
      try {
        Scanner scanner = new Scanner(file);
        while (scanner.hasNext()) {
          String line = scanner.nextLine();
          if (skip(excludeDatabases, excludeTables, beginDate, endDate, line)) {
            continue;
          }

          Path path = new Path(StringUtils.strip(pathPrefix + line));
          pool.submit(() -> {
            try {
              if (dryRun) {
                LOG.info("TO DELETE: {}", path);
              } else {
                FileSystem fs = path.getFileSystem(conf);
                fs.delete(path);
                LOG.info("DELETED: {}", path);
              }
            } catch (Exception e) {
              LOG.error("delete path failed", e);
            }
          });
        }
      } catch (java.text.ParseException | IOException e) {
        LOG.error(String.format("process file:%s filed", file.getPath()), e);
      }

      return new Object();
    });

    pool.shutdown();
    pool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
  }

  private static boolean skip(Set<String> excludeDatabases, Set<String> excludeTables,
      Date beginDate, Date endDate, String line) throws java.text.ParseException {
    if (StringUtils.isEmpty(line)) {
      return true;
    }
    Item item = parser(line);
    if (excludeDatabases.contains(item.db)) {
      return true;
    }
    if (excludeTables.contains(item.table)) {
      return true;
    }

    if (item.partition != null && item.partition.getKey().equals("date")) {
      Date date = null;
      try {
        date = DateUtil.parseDate(item.partition.getValue());
      } catch (Exception e) {
        LOG.error("parse date failed", e);
      }

      if (date != null && beginDate != null && date.before(beginDate)) {
        return true;
      }

      if (date != null && endDate != null && date.after(endDate)) {
        return true;
      }
    }
    return false;
  }

  private static Item parser(String line) {
    String[] str = line.split("/");
    Item item = new Item();
    if (str.length >= 2) {
      item.db = str[1].split("\\.")[0];
    }

    if (str.length >= 3) {
      item.table = str[2];
    }

    if (str.length >= 4 && str[3].contains("=")) {
      String[] part = str[3].split("=");
      item.partition = Pair.of(part[0], part[1]);
    }

    return item;
  }

  private static void findFile(File root, Function<File, Object> function) {
    if (root.isDirectory()) {
      for (File file : Objects.requireNonNull(root.listFiles())) {
        if (file.isDirectory()) {
          findFile(file, function);
        } else {
          function.apply(file);
        }
      }
    } else {
      function.apply(root);
    }
  }

  static class Item {

    String db;
    String table;
    Pair<String, String> partition;
  }
}

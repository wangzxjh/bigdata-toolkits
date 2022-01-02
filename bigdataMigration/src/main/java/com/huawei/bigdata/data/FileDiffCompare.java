package com.huawei.bigdata.data;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

public class FileDiffCompare {

  public static final String TARGET_DIR = "TARGET_DIR";
  public static final String SOURCE_DIR = "SOURCE_DIR";
  public static final String DRY_RUN = "DRY_RUN";

  public static void main(String[] args) throws ParseException {
    Options options = new Options();
    options.addRequiredOption(TARGET_DIR, TARGET_DIR, true, "文件比较的目标目录");
    options.addRequiredOption(SOURCE_DIR, TARGET_DIR, true, "文件比较的源目录");
    options.addOption(DRY_RUN, DRY_RUN, true, "是否空跑");
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    String targetDir = cmd.getOptionValue(TARGET_DIR);
    String sourceDir = cmd.getOptionValue(SOURCE_DIR);
    boolean dryRun = Boolean.parseBoolean(cmd.getOptionValue(DRY_RUN,"true"));

    Configuration conf = new Configuration();
  }
}

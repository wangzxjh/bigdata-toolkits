package com.huawei.bigdata.cdm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.GenericDeclaration;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdmBatchSqlDecode {

  public static final Logger LOGGER = LoggerFactory.getLogger(CdmBatchSqlDecode.class);

  public static final String CDM_EXPORT_FILE_PATH = "cdmExportPath";
  public static final String OUTPUT_FILE = "outputFile";

  public static final Pattern PATTERN = Pattern.compile("\\s*|\t|\r|\n");
  public static final Pattern REPLACE_TIME_PATTERN = Pattern.compile("time`,char\\(|date`,char\\(");

  public static void main(String[] args) throws ParseException {
    Options options = new Options();
    options.addRequiredOption(CDM_EXPORT_FILE_PATH, CDM_EXPORT_FILE_PATH, true, "CMD导出文件路径");
    options.addRequiredOption(OUTPUT_FILE, OUTPUT_FILE, true, "结果文件路径");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);
    String cdmExportFilePath = cmd.getOptionValue(CDM_EXPORT_FILE_PATH);

    File cdmExportFile = new File(cdmExportFilePath);
    if (!cdmExportFile.exists()) {
      throw new RuntimeException(String.format("File %s does not exists", cdmExportFilePath));
    }

    String outFilePath = cmd.getOptionValue(OUTPUT_FILE);
    File outFile = new File(outFilePath);
//    if (outFile.exists()) {
//      throw new RuntimeException(String.format("output file %s already exists", outFile));
//    }

    ObjectMapper mapper = new ObjectMapper();
    try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(outFile))) {
      JsonNode jsonNode = mapper.readTree(cdmExportFile);
      JsonNode jobs = jsonNode.get("jobs");
      for (int i = 0; i < jobs.size(); i++) {
        JsonNode job = jobs.get(i);

        String name = job.get("name").asText();
        // get encodeSql
        String encodeSql = extractSql(job);
        if (StringUtils.isEmpty(encodeSql)) {
          continue;
        }
        LOGGER.info("job name:{}, encode sql:{}", name, encodeSql);

        String decodeSql = Base64Utils.decodeUtf8(encodeSql);
        decodeSql = strip(decodeSql);

        LOGGER.info("job name:{}, decode sql:{}", name, decodeSql);
        // 是否包含replace datetime
        if (!containsReplaceTime(decodeSql)) {
          continue;
        }

        outputStream.write((cdmExportFile.getName().split("\\.")[0] + "\t" + name + "\t" + decodeSql + "\n")
            .getBytes(StandardCharsets.UTF_8));

      }
    } catch (IOException e) {
      LOGGER.info("error", e);
    }
  }

  private static boolean containsReplaceTime(String decodeSql) {
    Matcher m = REPLACE_TIME_PATTERN.matcher(decodeSql.toLowerCase(Locale.ROOT));
    return m.find();
  }

  private static String strip(String decodeSql) {
    Matcher m = PATTERN.matcher(decodeSql);
    return m.replaceAll("");
  }

  private static String extractSql(JsonNode job) {
    String encodeSql = null;
    JsonNode configs = job.get("from-config-values").get("configs");
    for (int j = 0; j < configs.size(); j++) {
      JsonNode inputs = configs.get(j).get("inputs");
      for (int k = 0; k < inputs.size(); k++) {
        String name = inputs.get(k).get("name").asText();
        if ("fromJobConfig.sql".equals(name)) {
          encodeSql = inputs.get(k).get("value").asText();
          break;
        }
      }
      if (encodeSql != null) {
        break;
      }
    }
    return encodeSql;
  }

}

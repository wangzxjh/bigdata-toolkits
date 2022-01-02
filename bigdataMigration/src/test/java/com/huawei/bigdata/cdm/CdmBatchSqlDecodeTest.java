package com.huawei.bigdata.cdm;

import java.io.IOException;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

public class CdmBatchSqlDecodeTest {

  @Test
  public void test1() throws ParseException, IOException {
    String path = "D:\\task\\搜科网\\cdm\\1225\\48a8.json";
    String outPath = "D:\\task\\搜科网\\cdm\\1225\\48a8.result";
    String arg = String.format("-%s %s -%s %s", CdmBatchSqlDecode.CDM_EXPORT_FILE_PATH, path,
        CdmBatchSqlDecode.OUTPUT_FILE, outPath);
    CdmBatchSqlDecode.main(arg.split(" "));
  }

  @Test
  public void test2() throws ParseException, IOException {
    String path = "D:\\task\\搜科网\\cdm\\1225\\3885.json";
    String outPath = "D:\\task\\搜科网\\cdm\\1225\\3885.result";
    String arg = String.format("-%s %s -%s %s", CdmBatchSqlDecode.CDM_EXPORT_FILE_PATH, path,
        CdmBatchSqlDecode.OUTPUT_FILE, outPath);
    CdmBatchSqlDecode.main(arg.split(" "));
  }
}
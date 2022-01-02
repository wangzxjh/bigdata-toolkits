package com.huawei.bigdata.data;

import org.apache.commons.cli.ParseException;
import org.junit.Test;

public class BatchDeleteTest {

  @Test
  public void test1() throws ParseException, java.text.ParseException, InterruptedException {
    String path = BatchDeleteTest.class.getResource("/output").getFile();
    String arg = "-toDeleteSourceDir " + path + " "
        + "-threads 8 "
        + "-excludeDatabase ods,log "
        + "-excludeTables abc "
        + "-pathPrefix obs://xxx  -beginDate 2021-12-01 -endDate 2021-12-21 -dryRun true";
    BatchDelete.main(arg.split(" "));
  }

  @Test
  public void test2() throws ParseException, java.text.ParseException, InterruptedException {
    String path = BatchDeleteTest.class.getResource("/output").getFile();
    String arg = "-toDeleteSourceDir " + path + " "
        + "-threads 8 "
        + "-excludeDatabase log "
        + "-excludeTables abc "
        + "-pathPrefix obs://xxx  -beginDate 2021-12-01 -endDate 2021-12-21 -dryRun true";
    BatchDelete.main(arg.split(" "));
  }

  @Test
  public void test3() throws ParseException, java.text.ParseException, InterruptedException {
    String path = BatchDeleteTest.class.getResource("/output").getFile();
    String arg = "-toDeleteSourceDir " + path + " "
        + "-threads 8 "
        + "-excludeDatabase log "
        + "-excludeTables rpt_plat_flow_shopuser_daily "
        + "-pathPrefix obs://xxx  -beginDate 2021-12-01 -endDate 2021-12-21 -dryRun true";
    BatchDelete.main(arg.split(" "));
  }

  @Test
  public void test4() throws ParseException, java.text.ParseException, InterruptedException {
    String path = BatchDeleteTest.class.getResource("/output").getFile();
    String arg = "-toDeleteSourceDir " + path + " "
        + "-threads 8 "
        + "-excludeDatabase log "
        + "-excludeTables xxx "
        + "-pathPrefix obs://xxx  -beginDate 2021-12-02 -endDate 2021-12-18 -dryRun true";
    BatchDelete.main(arg.split(" "));
  }
}
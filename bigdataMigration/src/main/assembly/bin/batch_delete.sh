#!/bin/bash

SHELL_FOLDER=$(cd "$(dirname "$0")";pwd)
HOME=$SHELL_FOLDER/..
echo $HOME


java  -cp $HOME/lib/*:$HOME/conf com.huawei.bigdata.data.BatchDelete \
  -toDeleteSourceDir "/opt/file-compare-tool-1.0-SNAPSHOT/output" \
  -threads 8 \
  -excludeDatabase ods,log \
  -excludeTables abc \
  -pathPrefix obs://vvic-bigdata/user/hive/warehouse \
  -beginDate 2021-12-12 \
  -endDate 2021-12-22 \
  -dryRun true

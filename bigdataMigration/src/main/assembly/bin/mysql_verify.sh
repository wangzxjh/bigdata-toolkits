#!/bin/bash

SHELL_FOLDER=$(cd "$(dirname "$0")";pwd)
HOME=$SHELL_FOLDER/..
echo $HOME


java  -cp $HOME/lib/*:$HOME/conf com.huawei.bigdata.verify.mysql.MysqlVerify

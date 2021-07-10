package org.huawei.jdbc.mysql;

import org.hamcrest.core.Is;
import org.huawei.jdbc.JdbcClient;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

public class JobClientTest {
    protected JdbcClient client;


    public void baseCase() throws SQLException {

        client.execute("create database if not exists test");
        client.execute("use test");
        client.execute("drop table if exists test");
        client.execute("create table test(id varchar(10),name varchar(10))");
        client.executeUpdate("insert into test values('1','a'),('2','b')",
                number -> Assert.assertThat(number, Is.is(2)));

        client.executeUpdate("insert into test values('3','c'),('4','d')",
                number -> Assert.assertThat(number, Is.is(2)));

        client.executeQuery("select count(1) as cnt from test", rs -> {
            while (rs.next()) {
                Assert.assertThat(rs.getInt("cnt"), Is.is(4));
            }
        });

    }
}

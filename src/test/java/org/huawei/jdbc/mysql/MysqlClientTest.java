package org.huawei.jdbc.mysql;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;

public class MysqlClientTest extends JobClientTest {
    String url = "jdbc:mysql://localhost:3306/";
    String user = "root";
    String passwd = "123456";

    @Before
    public void setup() throws SQLException, ClassNotFoundException {
        client = new MysqlClient(url, user, passwd);
        client.init();
    }

    @Ignore
    public void testCase() throws SQLException {
        baseCase();
    }

    @After
    public void tearDown() throws Exception {
        client.close();
    }
}
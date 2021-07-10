package org.huawei.jdbc.mysql;

import org.huawei.jdbc.JdbcClient;

import java.util.Properties;

public class MysqlClient extends JdbcClient {

    public MysqlClient(String url, String user, String passwd) {
        super(url, user, passwd);
    }

    public MysqlClient(String url, String user, String passwd, Properties... properties) {
        super(url, user, passwd, properties);
    }

    public MysqlClient(String url, Properties properties) {
        super(url, properties);
    }

    @Override
    public void loadDriverClass() throws ClassNotFoundException {
//        Class.forName("com.mysql.jdbc.Driver");
    }
}

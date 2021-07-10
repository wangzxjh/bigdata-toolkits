package org.huawei.jdbc.hetu;

import org.huawei.jdbc.JdbcClient;

import java.util.Properties;

public class HetuClient extends JdbcClient {

    private HetuClient(String url, String user, String passwd) {
        super(url, user, passwd);
    }

    public HetuClient(String url, String user, String passwd, Properties... properties) {
        super(url, user, passwd, properties);
    }

    public HetuClient(String url, Properties properties) {
        super(url, properties);
    }


    @Override
    protected void loadDriverClass() throws ClassNotFoundException {
        Class.forName("io.prestosql.jdbc.PrestoDriver");
    }

}

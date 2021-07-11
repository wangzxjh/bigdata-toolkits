package org.huawei.jdbc.hetu;

import org.huawei.jdbc.JdbcClient;
import org.huawei.jdbc.mysql.JobClientTest;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Objects;

import static org.huawei.jdbc.hetu.HetuClientFactory.KRB_5_CONF_FILENAME;
import static org.junit.Assert.*;

public class HetuClientTest extends JobClientTest {

    @Ignore
    public void normalBrokerClient() throws SQLException, ClassNotFoundException {
        String url = "jdbc:presto://192.168.1.130:29861,192.168.1.131:29861,192.168.1.132:29861/hive/default";
        String user = "root";
        client = HetuClientFactory.createNormalInstance(url, user, DiscoverMode.BROKER);
        client.init();

        baseCase();
    }


    @Ignore
    public void normalZKClient() throws SQLException, ClassNotFoundException {
        String url = "jdbc:presto://192.168.1.130:24002,192.168.1.131:24002,192.168.1.132:24002/hive/default";
        String user = "root";
        client = HetuClientFactory.createNormalInstance(url, user, DiscoverMode.ZOOKEEPER);
        client.init();

        baseCase();
    }


    @Ignore
    @Test   
    public void securityZKClient1() throws SQLException, ClassNotFoundException {
        String url = "jdbc:presto://192.168.0.66:24002/hive/default";
        String user = "wangzhen";
        client = HetuClientFactory.createSecurityZkInstance(url, user);
        client.init();

        baseCase();
    }


    @Test
    public void securityZKPswdClient() throws SQLException, ClassNotFoundException {
        String url = "jdbc:presto://192.168.0.66:24002/hive/default";
        String user = "wangzhen";
        String passwd = "CTPlat!@09";
        client = HetuClientFactory.createSecurityZkPasswordInstance(url, user, passwd);
        client.init();
        baseCase();
    }

    @Ignore
    @Test
    public void securityBrokerClient() throws SQLException, ClassNotFoundException {
        String url = "jdbc:presto://192.168.0.66:29860/default/default";
        String user = "wangzhen";
        String passwd = "CTPlat!@09";
        client = HetuClientFactory.createSecurityBrokerInstance(url, user, passwd);
        client.init();
        baseCase();
    }
}
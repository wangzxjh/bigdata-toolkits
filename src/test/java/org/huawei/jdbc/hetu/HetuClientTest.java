package org.huawei.jdbc.hetu;

import org.huawei.jdbc.JdbcClient;
import org.huawei.jdbc.mysql.JobClientTest;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Objects;

import static org.huawei.jdbc.hetu.HetuClientFactory.KRB_5_CONF_FILENAME;
import static org.junit.Assert.*;

public class HetuClientTest extends JobClientTest {

    @Test
    public void normalBrokerClient() throws SQLException, ClassNotFoundException {
        String url = "jdbc:presto://192.168.1.130:29861,192.168.1.131:29861,192.168.1.132:29861/hive/default";
        String user = "root";
        client = HetuClientFactory.createNormalInstance(url, user, DiscoverMode.BROKER);
        client.init();

        baseCase();
    }


    @Test
    public void normalZKClient() throws SQLException, ClassNotFoundException {
        String url = "jdbc:presto://192.168.1.130:24002,192.168.1.131:24002,192.168.1.132:24002/hive/default";
        String user = "root";
        client = HetuClientFactory.createNormalInstance(url, user, DiscoverMode.ZOOKEEPER);
        client.init();

        baseCase();
    }


    @Test
    public void securityZKClient1() throws SQLException, ClassNotFoundException {
        String url = "jdbc:presto://192.168.1.130:24002,192.168.1.131:24002,192.168.1.132:24002/hive/default";
        String user = "root";
        client = HetuClientFactory.createSecurityZkInstance(url, user);
        client.init();

        baseCase();
    }

    @Test
    public void securityZKClient2() throws SQLException, ClassNotFoundException {
        String url = "jdbc:presto://192.168.1.130:24002,192.168.1.131:24002,192.168.1.132:24002/hive/default";
        String user = "root";
        client = HetuClientFactory.createSecurityZkInstance(url, user);
        client.init();

        baseCase();
    }

    @Test
    public void securityZKPswdClient() throws SQLException, ClassNotFoundException {
        String url = "jdbc:presto://192.168.1.130:24002,192.168.1.131:24002,192.168.1.132:24002/hive/default";
        String user = "root";
        String passwd = "";
        String krb5Path = Objects.requireNonNull(HetuClientFactory.class.getClassLoader()
                .getResource(KRB_5_CONF_FILENAME))
                .getPath();
        client = HetuClientFactory.createSecurityZkPasswordInstance(url, user, passwd, krb5Path);
        client.init();
        baseCase();
    }


    @Test
    public void securityBrokerClient() throws SQLException, ClassNotFoundException {
        String url = "jdbc:presto://192.168.1.130:29860,192.168.1.131:29860,192.168.1.132:29860/hive/default";
        String user = "root";
        String passwd = "";
        client = HetuClientFactory.createSecurityBrokerInstance(url, user, passwd);
        client.init();
        baseCase();
    }
}
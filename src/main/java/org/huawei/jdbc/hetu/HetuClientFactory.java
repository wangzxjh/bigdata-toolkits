package org.huawei.jdbc.hetu;

import static org.huawei.jdbc.hetu.DiscoverMode.BROKER;
import static org.huawei.jdbc.hetu.DiscoverMode.ZOOKEEPER;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.huawei.jdbc.JdbcClient;
public class HetuClientFactory {
    public static final String SERVICE_DISCOVERY_MODE = "serviceDiscoveryMode";
    public static final String SSL = "SSL";
    public static final String ZOO_KEEPER_NAMESPACE = "zooKeeperNamespace";
    public static final String TENANT = "tenant";
    public static final String DEPLOYMENT_MODE = "deploymentMode";
    public static final String ZOO_KEEPER_AUTH_TYPE = "ZooKeeperAuthType";
    public static final String KERBEROS_CONFIG_PATH = "KerberosConfigPath";
    public static final String KERBEROS_PRINCIPAL = "KerberosPrincipal";
    public static final String KERBEROS_KEYTAB_PATH = "KerberosKeytabPath";
    public static final String KERBEROS_REMOTE_SERVICE_NAME = "KerberosRemoteServiceName";
    public static final String ZOO_KEEPER_SASL_CLIENT_CONFIG = "ZooKeeperSaslClientConfig";
    public static final String JAAS_ZK_CONF_FILE_NAME = "jaas-zk.conf";
    public static final String KRB_5_CONF_FILENAME = "krb5.conf";
    public static final String USER_KEYTAB_FILE_NAME = "user.keytab";

    private static JdbcClient createNormalBrokerInstance(String url, String user) {
        url = appendNeededParameter(url, BROKER);

        Properties properties = new Properties();
        properties.setProperty(HetuClient.USER, user);
        properties.setProperty(SSL, "false");

        return new HetuClient(url, properties);
    }

    public static class ParsedURL {
        private final String url;
        private final Map<String, String> parameters;

        public ParsedURL(String url, Map<String, String> parameters) {
            this.url = url;
            this.parameters = parameters;
        }

        public String getUrl() {
            return url;
        }

        public Map<String, String> getParameters() {
            return parameters;
        }

        public static ParsedURL parse(String url) {
            String[] parts = url.split("\\?");
            url = parts[0];

            HashMap<String, String> parameters = new HashMap<>();
            if (parts.length > 1) {
                Arrays.stream(parts[1].split("&")).forEach(str -> {
                    String[] kv = str.split("=");
                    parameters.put(kv[0], kv[1]);
                });
            }
            return new ParsedURL(url, parameters);
        }

        @Override
        public String toString() {
            if (parameters.isEmpty()) {
                return url;
            }
            StringBuilder builder = new StringBuilder(url);
            builder.append("?");
            parameters.forEach((k, v) -> builder.append(k).append("=").append(v).append("&"));
            return builder.substring(0, builder.length() - 1);
        }
    }

    private static String appendNeededParameter(String url, DiscoverMode discoverMode) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(url), "url can not be empty");

        ParsedURL parsedURL = ParsedURL.parse(url);

        String value = parsedURL.getParameters().get(SERVICE_DISCOVERY_MODE);
        if (value != null) {
            Preconditions.checkArgument(discoverMode.getValue().equals(value),
                    SERVICE_DISCOVERY_MODE + " must be " + discoverMode.getValue() + ",but actually is " + value);
        } else {
            parsedURL.getParameters().put(SERVICE_DISCOVERY_MODE, discoverMode.getValue());
        }


        if (discoverMode == ZOOKEEPER) {
            parsedURL.getParameters().put(ZOO_KEEPER_NAMESPACE, BROKER.getValue());
        }

        return parsedURL.toString();
    }

    private static JdbcClient createNormalZkInstance(String url, String user) {
        url = appendNeededParameter(url, ZOOKEEPER);

        Properties properties = new Properties();
        properties.setProperty(HetuClient.USER, user);
        properties.setProperty(TENANT, "default");
        properties.setProperty(DEPLOYMENT_MODE, "on_yarn");
        properties.setProperty(ZOO_KEEPER_AUTH_TYPE, "simple");

        return new HetuClient(url, properties);
    }

    public static JdbcClient createNormalInstance(String url, String user, DiscoverMode discoverMode) {
        if (discoverMode == BROKER) {
            return createNormalBrokerInstance(url, user);
        } else {
            return createNormalZkInstance(url, user);
        }
    }

    public static JdbcClient createSecurityZkInstance(String url, String user, String jaasZkPath, String krb5Path, String userKeytabPath) {
        url = appendNeededParameter(url, ZOOKEEPER);

        Properties properties = new Properties();
        System.setProperty("user.timezone", "UTC");
        System.setProperty("java.security.auth.login.config", jaasZkPath);
        System.setProperty("java.security.krb5.conf", krb5Path);
        properties.setProperty(HetuClient.USER, user);
        properties.setProperty(SSL, "true");
        properties.setProperty(KERBEROS_CONFIG_PATH, krb5Path);
        properties.setProperty(KERBEROS_PRINCIPAL, user);
        properties.setProperty(KERBEROS_KEYTAB_PATH, userKeytabPath);
        properties.setProperty(KERBEROS_REMOTE_SERVICE_NAME, "HTTP");
        properties.setProperty(TENANT, "default");
        properties.setProperty(DEPLOYMENT_MODE, "on_yarn");
        properties.setProperty(ZOO_KEEPER_AUTH_TYPE, "kerberos");
        properties.setProperty(ZOO_KEEPER_SASL_CLIENT_CONFIG, "Client");

        return new HetuClient(url, properties);
    }

    public static JdbcClient createSecurityZkInstance(String url, String user) {
        String jaasZkPath = Objects.requireNonNull(HetuClientFactory.class.getClassLoader()
                .getResource(JAAS_ZK_CONF_FILE_NAME))
                .getPath();
        String krb5Path = Objects.requireNonNull(HetuClientFactory.class.getClassLoader()
                .getResource(KRB_5_CONF_FILENAME))
                .getPath();
        String userKeytabPath = Objects.requireNonNull(HetuClientFactory.class.getClassLoader()
                .getResource(USER_KEYTAB_FILE_NAME))
                .getPath();
        return createSecurityZkInstance(url, user, jaasZkPath, krb5Path, userKeytabPath);
    }

    public static JdbcClient createSecurityZkPasswordInstance(String url, String user, String password) {
        url = appendNeededParameter(url, ZOOKEEPER);
        String krb5Path = Objects.requireNonNull(HetuClientFactory.class.getClassLoader()
                .getResource(KRB_5_CONF_FILENAME))
                .getPath();

        Properties properties = new Properties();
        System.setProperty("user.timezone", "UTC");
        System.setProperty("java.security.krb5.conf", krb5Path);
        properties.setProperty(HetuClient.USER, user);
        properties.setProperty(HetuClient.PASSWORD, password);
        properties.setProperty(SSL, "true");
        properties.setProperty(KERBEROS_CONFIG_PATH, krb5Path);
        properties.setProperty(KERBEROS_REMOTE_SERVICE_NAME, "HTTP");
        properties.setProperty(TENANT, "default");
        properties.setProperty(DEPLOYMENT_MODE, "on_yarn");

        return new HetuClient(url, properties);
    }

    public static JdbcClient createSecurityBrokerInstance(String url, String user, String password) {
        url = appendNeededParameter(url, BROKER);
        Properties properties = new Properties();
        properties.setProperty(HetuClient.USER, user);
        properties.setProperty(HetuClient.PASSWORD, password);

        return new HetuClient(url, properties);
    }
}

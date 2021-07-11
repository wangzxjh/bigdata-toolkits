package org.huawei.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.huawei.security.LoginUtil;

import java.io.IOException;
import java.util.Objects;

public class ResourceManagerClientFactory {


    public ResourceManagerClientFactory() {
    }

    public static ResourceManagerAPI createNormalInstance() {
        Configuration conf = new YarnConfiguration();
        return new ResourceManagerClient(conf);
    }

    public static ResourceManagerAPI createNormalInstance(String address) {
        Configuration conf = new YarnConfiguration();
        conf.set(YarnConfiguration.RM_ADDRESS, address);
        return new ResourceManagerClient(conf);
    }

    public static ResourceManagerAPI createSecurityInstance(String principalName) throws IOException {
        Configuration conf = new YarnConfiguration();
        System.out.println(conf.get("yarn.resourcemanager.resource-tracker.address.port"));
        login(principalName, conf);
        return new ResourceManagerClient(conf);
    }

    public static ResourceManagerClient createSecurityInstance(String address, String principalName) throws IOException {
        Configuration conf = new YarnConfiguration();
        conf.set(YarnConfiguration.RM_ADDRESS, address);
        login(principalName, conf);
        return new ResourceManagerClient(conf);
    }

    private static void login(String principalName, Configuration conf) throws IOException {
        String PATH_TO_KEYTAB = Objects.requireNonNull(ResourceManagerClientFactory.class.getClassLoader()
                .getResource("user.keytab"))
                .getPath();

        String PATH_TO_KRB5_CONF = Objects.requireNonNull(ResourceManagerClientFactory.class.getClassLoader()
                .getResource("krb5.conf"))
                .getPath();

        LoginUtil.login(principalName, PATH_TO_KEYTAB, PATH_TO_KRB5_CONF, conf);
    }
}

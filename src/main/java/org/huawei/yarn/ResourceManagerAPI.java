package org.huawei.yarn;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.huawei.yarn.entity.ApplicationInfo;

import java.io.IOException;
import java.util.List;

public interface ResourceManagerAPI {
    ApplicationInfo getApplication(String Id) throws IOException, YarnException;

    List<ApplicationInfo> getAllApplications() throws IOException, YarnException;

    List<ApplicationInfo> queryApplication(Filter filter) throws IOException, YarnException;
}

package org.huawei.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.huawei.yarn.entity.ApplicationInfo;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class ResourceManagerClient implements ResourceManagerAPI {
    private YarnClient yarnClient;

    private ResourceManagerClient() {
    }


    ResourceManagerClient(Configuration conf) {
        this.yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
    }


    @Override
    public ApplicationInfo getApplication(String id) throws IOException, YarnException {
        ApplicationReport applicationReport = yarnClient.getApplicationReport(ApplicationId.fromString(id));
        return Converters.INSTANCE.convertToApplicationInfo(applicationReport);
    }


    @Override
    public List<ApplicationInfo> getAllApplications() throws IOException, YarnException {
        return yarnClient.getApplications().stream().map(Converters.INSTANCE::convertToApplicationInfo).collect(Collectors.toList());
    }

    @Override
    public List<ApplicationInfo> queryApplication(Filter filter) throws IOException, YarnException {
        GetApplicationsRequest request = GetApplicationsRequest.newInstance();
        Optional.ofNullable(filter.getQueues()).ifPresent(request::setQueues);
        Optional.ofNullable(filter.getUsers()).ifPresent(request::setUsers);
        Optional.ofNullable(filter.getApplicationTypes()).ifPresent(request::setApplicationTypes);
        Optional.ofNullable(filter.getApplicationTags()).ifPresent(request::setApplicationTags);
        Optional.ofNullable(filter.getApplicationStates()).ifPresent(t -> request.setApplicationStates(Converters.INSTANCE.convertToYarnApplicationState(t)));
        Optional.ofNullable(filter.getStartRange()).ifPresent(request::setStartRange);
        Optional.ofNullable(filter.getFinishRange()).ifPresent(request::setFinishRange);
        Optional.ofNullable(filter.getLimit()).ifPresent(request::setLimit);

        return yarnClient.getApplications(request).stream().map(Converters.INSTANCE::convertToApplicationInfo).collect(Collectors.toList());
    }

    @Override
    public void close() throws Exception {
        yarnClient.close();
    }

}

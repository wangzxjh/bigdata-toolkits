package org.huawei.yarn;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.huawei.yarn.entity.ApplicationInfo;
import org.huawei.yarn.entity.ApplicationResourceUsage;
import org.huawei.yarn.entity.ApplicationState;
import org.huawei.yarn.entity.FinalApplicationStatus;

import java.util.EnumSet;

enum Converters {
    INSTANCE;

    EnumSet<YarnApplicationState> convertToYarnApplicationState(EnumSet<ApplicationState> states) {
        EnumSet<YarnApplicationState> result = EnumSet.noneOf(YarnApplicationState.class);
        states.forEach(s -> {
            result.add(YarnApplicationState.valueOf(s.name()));
        });
        return result;
    }

    ApplicationInfo convertToApplicationInfo(ApplicationReport report) {
        ApplicationInfo info = new ApplicationInfo();
        info.setApplicationId(report.getApplicationId().toString());
        info.setApplicationAttemptId(report.getCurrentApplicationAttemptId().toString());
        info.setUser(report.getUser());
        info.setQueue(report.getQueue());
        info.setName(report.getName());
        info.setRpcPort(report.getRpcPort());
        info.setState(ApplicationState.valueOf(report.getYarnApplicationState().name()));
        info.setDiagnostics(report.getDiagnostics());
        info.setUrl(report.getTrackingUrl());
        info.setStartTime(report.getStartTime());
        info.setFinishTime(report.getFinishTime());
        info.setFinalStatus(FinalApplicationStatus.valueOf(report.getFinalApplicationStatus().name()));
        info.setAppResources(convertUsage(report.getApplicationResourceUsageReport()));
        info.setOrigTrackingUrl(report.getOriginalTrackingUrl());
        info.setProgress(report.getProgress());
        info.setApplicationType(report.getApplicationType());
        return info;
    }

    static ApplicationResourceUsage convertUsage(ApplicationResourceUsageReport report) {
        ApplicationResourceUsage usage = new ApplicationResourceUsage();
        usage.setClusterUsagePerc(report.getClusterUsagePercentage());
        usage.setNeededResources(convertToResource(report.getNeededResources()));
        usage.setQueueUsagePerc(report.getQueueUsagePercentage());
        usage.setReservedResources(convertToResource(report.getReservedResources()));
        usage.setUsedResources(convertToResource(report.getUsedResources()));
        usage.setResourceSecondsMap(report.getResourceSecondsMap());
        usage.setNumReservedContainers(report.getNumReservedContainers());
        usage.setNumUsedContainers(report.getNumUsedContainers());
        usage.setPreemtedResourceSecondsMap(report.getPreemptedResourceSecondsMap());
        return usage;
    }

    static ApplicationResourceUsage.Resource convertToResource(Resource resource) {
        return new ApplicationResourceUsage.Resource(resource.getVirtualCores(), resource.getMemorySize());
    }
}

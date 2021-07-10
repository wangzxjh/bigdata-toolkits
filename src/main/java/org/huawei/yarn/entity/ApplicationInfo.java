package org.huawei.yarn.entity;

public class ApplicationInfo {
    private String applicationId;
    private String applicationAttemptId;
    private String user;
    private String queue;
    private String name;
    private int rpcPort;
    private ApplicationState state;
    private String diagnostics;
    private String url;
    private long startTime;
    private long finishTime;
    private FinalApplicationStatus finalStatus;
    private ApplicationResourceUsage appResources;
    private String origTrackingUrl;
    private float progress;
    private String applicationType;

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getApplicationAttemptId() {
        return applicationAttemptId;
    }

    public void setApplicationAttemptId(String applicationAttemptId) {
        this.applicationAttemptId = applicationAttemptId;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public ApplicationState getState() {
        return state;
    }

    public void setState(ApplicationState state) {
        this.state = state;
    }

    public String getDiagnostics() {
        return diagnostics;
    }

    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public FinalApplicationStatus getFinalStatus() {
        return finalStatus;
    }

    public void setFinalStatus(FinalApplicationStatus finalStatus) {
        this.finalStatus = finalStatus;
    }

    public ApplicationResourceUsage getAppResources() {
        return appResources;
    }

    public void setAppResources(ApplicationResourceUsage appResources) {
        this.appResources = appResources;
    }

    public String getOrigTrackingUrl() {
        return origTrackingUrl;
    }

    public void setOrigTrackingUrl(String origTrackingUrl) {
        this.origTrackingUrl = origTrackingUrl;
    }

    public float getProgress() {
        return progress;
    }

    public void setProgress(float progress) {
        this.progress = progress;
    }

    public String getApplicationType() {
        return applicationType;
    }

    public void setApplicationType(String applicationType) {
        this.applicationType = applicationType;
    }
}

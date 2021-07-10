package org.huawei.yarn.entity;

public enum ApplicationState {
    NEW,
    NEW_SAVING,
    SUBMITTED,
    ACCEPTED,
    RUNNING,
    FINISHED,
    FAILED,
    KILLED;

    private ApplicationState() {
    }
}

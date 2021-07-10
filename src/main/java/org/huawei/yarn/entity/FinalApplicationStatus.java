package org.huawei.yarn.entity;

public enum FinalApplicationStatus {
    UNDEFINED,
    SUCCEEDED,
    FAILED,
    KILLED,
    ENDED;

    private FinalApplicationStatus() {
    }
}

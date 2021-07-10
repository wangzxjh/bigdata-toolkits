package org.huawei.jdbc.hetu;

public enum DiscoverMode {
    BROKER("hsbroker"),
    ZOOKEEPER("zooKeeper");
    private String value;

    DiscoverMode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}

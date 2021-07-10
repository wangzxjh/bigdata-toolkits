package org.huawei.yarn.entity;

import java.util.Map;

public class ApplicationResourceUsage {
    private int numUsedContainers;
    private int numReservedContainers;
    private Resource usedResources;
    private Resource reservedResources;
    private Resource neededResources;
    private Map<String, Long> resourceSecondsMap;
    private float queueUsagePerc;
    private float clusterUsagePerc;
    private Map<String, Long> preemtedResourceSecondsMap;


    public int getNumUsedContainers() {
        return numUsedContainers;
    }

    public void setNumUsedContainers(int numUsedContainers) {
        this.numUsedContainers = numUsedContainers;
    }

    public int getNumReservedContainers() {
        return numReservedContainers;
    }

    public void setNumReservedContainers(int numReservedContainers) {
        this.numReservedContainers = numReservedContainers;
    }

    public Resource getUsedResources() {
        return usedResources;
    }

    public void setUsedResources(Resource usedResources) {
        this.usedResources = usedResources;
    }

    public Resource getReservedResources() {
        return reservedResources;
    }

    public void setReservedResources(Resource reservedResources) {
        this.reservedResources = reservedResources;
    }

    public Resource getNeededResources() {
        return neededResources;
    }

    public void setNeededResources(Resource neededResources) {
        this.neededResources = neededResources;
    }

    public Map<String, Long> getResourceSecondsMap() {
        return resourceSecondsMap;
    }

    public void setResourceSecondsMap(Map<String, Long> resourceSecondsMap) {
        this.resourceSecondsMap = resourceSecondsMap;
    }

    public float getQueueUsagePerc() {
        return queueUsagePerc;
    }

    public void setQueueUsagePerc(float queueUsagePerc) {
        this.queueUsagePerc = queueUsagePerc;
    }

    public float getClusterUsagePerc() {
        return clusterUsagePerc;
    }

    public void setClusterUsagePerc(float clusterUsagePerc) {
        this.clusterUsagePerc = clusterUsagePerc;
    }

    public Map<String, Long> getPreemtedResourceSecondsMap() {
        return preemtedResourceSecondsMap;
    }

    public void setPreemtedResourceSecondsMap(Map<String, Long> preemtedResourceSecondsMap) {
        this.preemtedResourceSecondsMap = preemtedResourceSecondsMap;
    }

    public static class Resource {
        private int vCores;
        private long memory;

        public Resource(int vCores, long memory) {
            this.vCores = vCores;
            this.memory = memory;
        }
    }


}

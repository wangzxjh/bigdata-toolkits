package org.huawei.yarn;

import org.apache.commons.lang.math.LongRange;
import org.huawei.yarn.entity.ApplicationState;

import java.util.EnumSet;
import java.util.Set;

public class Filter {
    private final Set<String> users;
    private final java.util.Set<String> queues;
    private final Set<String> applicationTypes;
    private final Set<String> applicationTags;
    private final EnumSet<ApplicationState> applicationStates;
    private final LongRange startRange;
    private final LongRange finishRange;
    private final Long limit;

    public Filter(Builder builder) {
        users = builder.users;
        queues = builder.queues;
        applicationTypes = builder.applicationTypes;
        applicationTags = builder.applicationTags;
        applicationStates = builder.applicationStates;
        startRange = builder.startRange;
        finishRange = builder.finishRange;
        limit = builder.limit;
    }

    public Set<String> getUsers() {
        return users;
    }

    public Set<String> getQueues() {
        return queues;
    }

    public Set<String> getApplicationTypes() {
        return applicationTypes;
    }

    public Set<String> getApplicationTags() {
        return applicationTags;
    }

    public EnumSet<ApplicationState> getApplicationStates() {
        return applicationStates;
    }

    public LongRange getStartRange() {
        return startRange;
    }

    public LongRange getFinishRange() {
        return finishRange;
    }

    public Long getLimit() {
        return limit;
    }

    public static class Builder {
        private Set<String> users;
        private Set<String> queues;
        private Set<String> applicationTypes;
        private Set<String> applicationTags;
        private EnumSet<ApplicationState> applicationStates;
        private LongRange startRange;
        private LongRange finishRange;
        private Long limit;


        public Builder setUsers(Set<String> users) {
            this.users = users;
            return this;
        }

        public Builder setQueues(Set<String> queues) {
            this.queues = queues;
            return this;
        }

        public Builder setApplicationTypes(Set<String> applicationTypes) {
            this.applicationTypes = applicationTypes;
            return this;
        }

        public Builder setApplicationTags(Set<String> applicationTags) {
            this.applicationTags = applicationTags;
            return this;
        }

        public Builder setApplicationStates(EnumSet<ApplicationState> applicationStates) {
            this.applicationStates = applicationStates;
            return this;
        }

        public Builder setStartRange(LongRange startRange) {
            this.startRange = startRange;
            return this;
        }

        public Builder setFinishRange(LongRange finishRange) {
            this.finishRange = finishRange;
            return this;
        }

        public Builder setLimit(Long limit) {
            this.limit = limit;
            return this;
        }
    }
}

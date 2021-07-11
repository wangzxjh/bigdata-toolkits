package org.huawei.yarn;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.Gson;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ResourceManagerClientTest {
    private ResourceManagerAPI resourceManagerAPI;

    @Before
    public void setUp() throws Exception {
        resourceManagerAPI = ResourceManagerClientFactory.createSecurityInstance("wangzhen@HADOOP.COM");
    }


    @Ignore
    @Test
    public void case1() throws IOException, YarnException {
        resourceManagerAPI.getAllApplications().forEach(info -> System.out.println(new Gson().toJson(info)));
    }

    
    @Test
    public void case2() throws IOException, YarnException {
        Filter filter = new Filter.Builder().setApplicationTypes(new HashSet(Arrays.asList("yarn-service"))).build();
        resourceManagerAPI.queryApplication(filter).forEach(info -> System.out.println(new Gson().toJson(info)));
    }

    @After
    public void tearDown() throws Exception {
        resourceManagerAPI.close();
    }
}
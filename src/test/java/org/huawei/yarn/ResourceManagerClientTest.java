package org.huawei.yarn;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ResourceManagerClientTest {
    private ResourceManagerAPI resourceManagerAPI;

    @Before
    public void setUp() throws Exception {
        resourceManagerAPI = ResourceManagerClientFactory.createSecurityInstance("localhost:26004","root");
    }


    @Test
    public void case1() throws IOException, YarnException {
        resourceManagerAPI.getAllApplications().forEach(info -> System.out.println(info.getApplicationId()));
    }

    @After
    public void tearDown() throws Exception {
        resourceManagerAPI.close();
    }
}
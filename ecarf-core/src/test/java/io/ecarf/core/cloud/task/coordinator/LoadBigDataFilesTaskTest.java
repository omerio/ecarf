package io.ecarf.core.cloud.task.coordinator;

import static org.junit.Assert.*;

import java.io.IOException;

import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudServiceImpl;
import io.ecarf.core.utils.TestUtils;

import org.junit.Before;
import org.junit.Test;

public class LoadBigDataFilesTaskTest {

    private EcarfGoogleCloudServiceImpl service;
    
    @Before
    public void setUp() throws Exception {
       
        this.service = new EcarfGoogleCloudServiceImpl();
        TestUtils.prepare(service);
    }

    @Test
    public void testRun() throws IOException {
        LoadBigDataFilesTask task = new LoadBigDataFilesTask();
        task.setCloudService(service);
        task.setBucket("swetodblp-fullrun-1");
        task.setTable("ontologies.swetodblp1");
        task.setEncode("true");
        task.run();
        
    }

}

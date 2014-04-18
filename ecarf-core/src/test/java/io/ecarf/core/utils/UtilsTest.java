package io.ecarf.core.utils;

import static org.junit.Assert.*;
import io.ecarf.core.cloud.VMMetaData;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UtilsTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testExceptionFromEcarfError() {
		String message = "/tmpswetodblp_2008_2.nt.gz (Permission denied)";
		String exception = "java.io.FileNotFoundException";
		String instanceId = "ecarf-evm-234";
		VMMetaData metadata = new VMMetaData();
		metadata.addValue(VMMetaData.ECARF_EXCEPTION, exception);
		metadata.addValue(VMMetaData.ECARF_MESSAGE, message);
		Exception e = Utils.exceptionFromEcarfError(metadata, instanceId);
		assertNotNull(e);
		assertEquals(Constants.EVM_EXCEPTION + instanceId, e.getMessage());
		assertNotNull(e.getCause());
		assertNotNull(e.getCause().getMessage());
		
		assertEquals(message, e.getCause().getMessage());
		
		try {
			throw e;
		} catch(Exception e1) {
			e1.printStackTrace();
			System.out.println(e.getCause());
		}
	}

}

package io.ecarf.core.utils;

import static org.junit.Assert.*;

import java.io.IOException;

import io.ecarf.core.cloud.VMMetaData;

import org.apache.commons.lang.StringUtils;
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
	
	@Test
	public void testCsvSplit() throws IOException {
		String csv = "<http://dblp.uni-trier.de/rec/bibtex/journals/micro/AndersonNS96>,\"http://dlib.computer.org/dynaweb/mi/mi1996/@ebt-link;hf=0?target=if(eq(query(%27%3CFNO%3E+cont+m1010%27),0),1,ancestor(ARTICLE,query(%27%3CFNO%3E+cont+m1010%27)))\"";
		String [] parts = StringUtils.split(csv, ',');
		assertTrue(parts.length > 2);
		
		parts = Utils.PARSER.parseLine(csv);
		assertEquals(2, parts.length);
		
		assertEquals("<http://dblp.uni-trier.de/rec/bibtex/journals/micro/AndersonNS96>", parts[0]);
		assertEquals("http://dlib.computer.org/dynaweb/mi/mi1996/@ebt-link;hf=0?target=if(eq(query(%27%3CFNO%3E+cont+m1010%27),0),1,ancestor(ARTICLE,query(%27%3CFNO%3E+cont+m1010%27)))", parts[1]);
	}

}

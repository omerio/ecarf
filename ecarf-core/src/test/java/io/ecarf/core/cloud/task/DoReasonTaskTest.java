/**
 * The contents of this file may be used under the terms of the Apache License, Version 2.0
 * in which case, the provisions of the Apache License Version 2.0 are applicable instead of those above.
 *
 * Copyright 2014, Ecarf.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.ecarf.core.cloud.task;

import io.ecarf.core.cloud.EcarfMetaData;
import io.ecarf.core.cloud.impl.google.GoogleCloudService;
import io.ecarf.core.cloud.task.processor.reason.phase2.DoReasonTask6;
import io.ecarf.core.cloud.types.TaskType;
import io.ecarf.core.utils.TestUtils;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DoReasonTaskTest {

	private GoogleCloudService service;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		this.service = new GoogleCloudService();
		TestUtils.prepare(service);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * ecarf-task=REASON, 
	 * ecarf-terms=<http://lsdis.cs.uga.edu/projects/semdis/opus#year>,<http://lsdis.cs.uga.edu/projects/semdis/opus#last_modified_date>,<http://lsdis.cs.uga.edu/projects/semdis/opus#author>,<http://lsdis.cs.uga.edu/projects/semdis/opus#ee>,<http://xmlns.com/foaf/0.1/Person>,<http://lsdis.cs.uga.edu/projects/semdis/opus#isIncludedIn>,<http://lsdis.cs.uga.edu/projects/semdis/opus#cites>,<http://lsdis.cs.uga.edu/projects/semdis/opus#Webpage>,<http://lsdis.cs.uga.edu/projects/semdis/opus#isbn>,<http://lsdis.cs.uga.edu/projects/semdis/opus#in_series>,<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter_of>,<http://lsdis.cs.uga.edu/projects/semdis/opus#gMonth>,<http://lsdis.cs.uga.edu/projects/semdis/opus#at_organization>,<http://xmlns.com/foaf/0.1/Document>,<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter>, 
	 * ecarf-schema=opus_august2007_closure.nt, 
	 * ecarf-table=ontologies.swetodblp, 
	 * ecarf-bucket=swetodblp

	 * LoadTaskTest.java
	 * @throws IOException 
	 */
	@Test
	public void testRun() throws IOException {
		EcarfMetaData metadata = new EcarfMetaData();
		metadata.addValue(EcarfMetaData.ECARF_TASK, TaskType.REASON_SINGLE_QUERY.toString());
		metadata.addValue(EcarfMetaData.ECARF_TABLE, "ontologies.swetodblp");
		metadata.addValue(EcarfMetaData.ECARF_BUCKET, "swetodblp");
		metadata.addValue(EcarfMetaData.ECARF_SCHEMA, "opus_august2007_closure.nt"); //Webpage, ee, cdrom, Proceedings, isbn
		metadata.addValue(EcarfMetaData.ECARF_TERMS, "<http://lsdis.cs.uga.edu/projects/semdis/opus#year>,"
				+ "<http://lsdis.cs.uga.edu/projects/semdis/opus#last_modified_date>,"
				+ "<http://lsdis.cs.uga.edu/projects/semdis/opus#author>,"
				+ "<http://lsdis.cs.uga.edu/projects/semdis/opus#ee>,"
				+ "<http://xmlns.com/foaf/0.1/Person>,"
				+ "<http://lsdis.cs.uga.edu/projects/semdis/opus#isIncludedIn>,"
				+ "<http://lsdis.cs.uga.edu/projects/semdis/opus#cites>,"
				+ "<http://lsdis.cs.uga.edu/projects/semdis/opus#Webpage>,"
				+ "<http://lsdis.cs.uga.edu/projects/semdis/opus#isbn>,"
				+ "<http://lsdis.cs.uga.edu/projects/semdis/opus#in_series>,"
				+ "<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter_of>,"
				+ "<http://lsdis.cs.uga.edu/projects/semdis/opus#gMonth>,"
				+ "<http://lsdis.cs.uga.edu/projects/semdis/opus#at_organization>,"
				+ "<http://xmlns.com/foaf/0.1/Document>,"
				+ "<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter>");//"<http://lsdis.cs.uga.edu/projects/semdis/opus#year>,<http://lsdis.cs.uga.edu/projects/semdis/opus#last_modified_date>,<http://lsdis.cs.uga.edu/projects/semdis/opus#author>,<http://lsdis.cs.uga.edu/projects/semdis/opus#ee>,<http://xmlns.com/foaf/0.1/Person>,<http://lsdis.cs.uga.edu/projects/semdis/opus#isIncludedIn>,<http://lsdis.cs.uga.edu/projects/semdis/opus#cites>,<http://lsdis.cs.uga.edu/projects/semdis/opus#Webpage>,<http://lsdis.cs.uga.edu/projects/semdis/opus#isbn>,<http://lsdis.cs.uga.edu/projects/semdis/opus#in_series>,<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter_of>,<http://lsdis.cs.uga.edu/projects/semdis/opus#gMonth>,<http://lsdis.cs.uga.edu/projects/semdis/opus#at_organization>,<http://xmlns.com/foaf/0.1/Document>,<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter>");
		DoReasonTask6 task = new DoReasonTask6(metadata, service);
		task.run();
	}

}

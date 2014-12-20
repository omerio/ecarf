package io.ecarf.core.cloud.impl.google;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableRow;

public class TestBigqueryStreaming {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws IOException {
		String projectId = "";
		String datasetId = "ontologies";
		String tableId = "";
		String timestamp = Long.toString((new Date()).getTime());
		
		Bigquery bigquery = null;
		TableRow row = new TableRow();
		row.set("column_name", 7.7);
		TableDataInsertAllRequest.Rows rows = new TableDataInsertAllRequest.Rows();
		rows.setInsertId(timestamp);
		
		rows.setJson(row);
		List  rowList =
		    new ArrayList();
		rowList.add(rows);
		TableDataInsertAllRequest content = 
		    new TableDataInsertAllRequest().setRows(rowList);
		TableDataInsertAllResponse response =
		    bigquery.tabledata().insertAll(
		        projectId, datasetId, tableId, content).execute();
	}

}

package io.ecarf.core.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Assert;
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
	public void testCsvSplit() throws IOException {
		String csv = "<http://dblp.uni-trier.de/rec/bibtex/journals/micro/AndersonNS96>,\"http://dlib.computer.org/dynaweb/mi/mi1996/@ebt-link;hf=0?target=if(eq(query(%27%3CFNO%3E+cont+m1010%27),0),1,ancestor(ARTICLE,query(%27%3CFNO%3E+cont+m1010%27)))\"";
		String [] parts = StringUtils.split(csv, ',');
		assertTrue(parts.length > 2);
		
		Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(new StringReader(csv));
		List<CSVRecord> recordsList = new ArrayList<>();
		for(CSVRecord record: records) {
			recordsList.add(record);
		}
		assertEquals(1, recordsList.size());
		
		parts = recordsList.get(0).values();
		
		assertEquals(2, parts.length);
		
		assertEquals("<http://dblp.uni-trier.de/rec/bibtex/journals/micro/AndersonNS96>", parts[0]);
		assertEquals("http://dlib.computer.org/dynaweb/mi/mi1996/@ebt-link;hf=0?target=if(eq(query(%27%3CFNO%3E+cont+m1010%27),0),1,ancestor(ARTICLE,query(%27%3CFNO%3E+cont+m1010%27)))", parts[1]);
	}
	
	@Test
	public void testNormalSentence() {
	    final String str1 = "Because";
	    final String str2 = "I'm";
	    final String str3 = "Batman";
	    final char delim = ' ';
	    final String text = str1 + delim + str2 + delim + str3;
	    List<String> parts = Utils.split(text, delim);
	    Assert.assertEquals(Arrays.asList(StringUtils.split(text, delim)), parts);
	}

	@Test
	public void testStartingWithDelim() {
	    final String str1 = "";
	    final String str2 = "I'm";
	    final String str3 = "Batman";
	    final char delim = ' ';
	    final String text = str1 + delim + str2 + delim + str3;
	    List<String> parts = Utils.split(text, delim);
	    Assert.assertEquals(Arrays.asList(StringUtils.split(text, delim)), parts);
	}
	
	@Test
	public void testSplitURL() {
	    final String url = "www.eurohandball.com/ech/men/2014/match/2/052/Netherlands+-+Sweden";
	    final char delim = '/';
	    List<String> parts = Utils.split(url, delim);
	    Assert.assertEquals(Arrays.asList(StringUtils.split(url, delim)), parts);
	}
	
	@Test
    public void testSplitURLNoSlash() {
        final String url = "www.eurohandball.com";
        final char delim = '/';
        List<String> parts = Utils.split(url, delim);
        Assert.assertEquals(Arrays.asList(StringUtils.split(url, delim)), parts);
    }
	
	@Test
    public void testSplitURLSlashAtEnd() {
        final String url = "www.eurohandball.com/";
        final char delim = '/';
        List<String> parts = Utils.split(url, delim);
        Assert.assertEquals(Arrays.asList(StringUtils.split(url, delim)), parts);
    }
	
	@Test
    public void testSplitURLSlashAtStartEnd() {
        final String url = "/www.eurohandball.com/";
        final char delim = '/';
        List<String> parts = Utils.split(url, delim);
        Assert.assertEquals(Arrays.asList(StringUtils.split(url, delim)), parts);
    }


	@Test
	public void testEmptyString() {
	    final String str1 = "";
	    final char delim = ' ';
	    List<String> parts = Utils.split(str1, delim);
	    Assert.assertEquals(Arrays.asList(StringUtils.split(str1, delim)), parts);
	}
	
	@Test
	public void testOnlyDelim() {
	    final String str1 = "";
	    final char delim = '/';
	    final String text = str1 + delim + delim;
	    List<String> parts = Utils.split(text, delim);
	    Assert.assertEquals(Arrays.asList(StringUtils.split(text, delim)), parts);
	}

	@Test
	public void testNotContainingDelim() {
	    final String str1 = "hello";
	    final char delim = 'x';
	    List<String> parts = Utils.split(str1, delim);
	    Assert.assertEquals(Arrays.asList(StringUtils.split(str1, delim)), parts);
	}

}

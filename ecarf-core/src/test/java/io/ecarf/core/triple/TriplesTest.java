package io.ecarf.core.triple;

import static org.junit.Assert.*;
import io.ecarf.core.utils.Constants;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.util.NxUtil;


/**
 * Testing issues with triple parsing from CSV
 * 		//<http://dblp.uni-trier.de/rec/bibtex/conf/gesellschaft/1988> <http://lsdis.cs.uga.edu/projects/semdis/opus#book_title> "Informatik und \"Dritte Welt\"" .
		// <http://dblp.uni-trier.de/rec/bibtex/conf/gesellschaft/1988>,"Informatik und \"Dritte
		//<http://dblp.uni-trier.de/rec/bibtex/tr/trier/MI99-17> <http://lsdis.cs.uga.edu/projects/semdis/opus#in_series> <http://example.org/UniversitatTrier,Mathematik_Informatik,Forschungsbericht> .
		//<http://dblp.uni-trier.de/rec/bibtex/tr/trier/MI99-17>,<http://example.org/UniversitatTrier,Mathematik_Informatik,Forschungsbericht>
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TriplesTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testParseNTriple() throws FileNotFoundException, IOException {
		
		String line = null;
		try (BufferedReader r = new BufferedReader(
				new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("test_data.nt"), Constants.UTF8))) {

			while ((line = r.readLine()) != null) {

				System.out.println(line);
				/*String[] terms = TripleUtils.parseNTriple(line);
				if(terms != null) {
					for(int i = 0; i < terms.length; i++) {
						// bigquery requires data to be properly escaped
						terms[i] = StringEscapeUtils.escapeCsv(terms[i]);
					}
					System.out.println(line);
					String result = StringUtils.join(terms, ',');
					System.out.println(result);
					List<String[]> text = new ArrayList<>();
					text.add(terms);
					String result = Utils.toCSV(text);
					System.out.println(result);
					String [] parsed = Utils.CSV_PARSER.parseLine(result);
					for(String parse: parsed) {
						System.out.println(parse);
					}
				}
				*/

			}
		}

	}

	@Test
	public void testParseNTriple1() throws IOException {
		
		
		try (BufferedReader reader = new BufferedReader(
				new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("test_data.nt"), Constants.UTF8))) {
			NxParser nxp = new NxParser(reader);

			while (nxp.hasNext())  {

				Node[] ns = nxp.next();
				if (ns.length == 3) {
					
					String[] terms = new String [3];
					//Only Process Triples  
					//Replace the print statements with whatever you want
					for (int i = 0; i < ns.length; i++)  {
						terms[i] = NxUtil.unescape(ns[i].toN3());//StringEscapeUtils.escapeCsv(ns[i].toN3());
						System.out.print(terms[i] + " ");
						
					}
					System.out.println(". ");
					
					List<String> termsList = new ArrayList<>();
					for(String term: terms) {
						termsList.add(StringEscapeUtils.escapeCsv(term));
					}
					String result = StringUtils.join(termsList, ',');
					System.out.println(result);
					
					/*String [] parsed = Utils.CSV_PARSER.parseLine(result);
					for(String parse: parsed) {
						System.out.println(parse);
					}*/
					Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(new StringReader(result));
					for (CSVRecord record : records) {
						for(int i = 0; i < record.size(); i++) {
							System.out.println(record.get(i));
							System.out.println(StringEscapeUtils.escapeCsv(record.get(i)));
						}
					}
					System.out.println("\n");
					
				}
			}
		}
		
		
		
	}
	
	@Test(expected = RuntimeException.class) 
	public void testCsvParser() throws IOException {
		
		try (BufferedReader r = new BufferedReader(
				new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("terms.csv"), Constants.UTF8))) {

			Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(r);
			for (CSVRecord record : records) {
				for(int i = 0; i < record.size(); i++) {
					System.out.println(record.get(i));
					System.out.println(StringEscapeUtils.escapeCsv(record.get(i)) + "\n");
				}
			}
			
		}
	}
	
	@Test
	public void testLoadGzippedCSVTriples() throws FileNotFoundException, IOException {
		Set<Triple> triples = 
				TripleUtils.loadCompressedCSVTriples("/Users/omerio/SkyDrive/PhD/Experiments/07_01_2015_SwetoDblp_5n_bigquery_direct/1420663781645.inf", false);
		assertNotNull(triples);
		assertFalse(triples.isEmpty());
		System.out.println(triples.size());
		System.out.println(triples.iterator().next());
		
	}

}

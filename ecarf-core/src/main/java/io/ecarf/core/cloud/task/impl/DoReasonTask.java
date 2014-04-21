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
package io.ecarf.core.cloud.task.impl;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.compress.compressors.gzip.GzipUtils;

import com.google.api.client.repackaged.com.google.common.base.Joiner;

import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.task.CommonTask;
import io.ecarf.core.reason.rulebased.GenericRule;
import io.ecarf.core.reason.rulebased.Rule;
import io.ecarf.core.term.TermUtils;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.triple.TripleUtils;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DoReasonTask extends CommonTask {

	
	public DoReasonTask(VMMetaData metadata, CloudService cloud) {
		super(metadata, cloud);
	}

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.Task#run()
	 */
	@Override
	public void run() throws IOException {
		
		String table = metadata.getValue(VMMetaData.ECARF_TABLE);
		Set<String> terms = metadata.getTerms();
		String schemaFile = metadata.getValue(VMMetaData.ECARF_SCHEMA);
		String bucket = metadata.getBucket();
		
		String localSchemaFile = Utils.TEMP_FOLDER + schemaFile;
		// download the file from the cloud storage
		this.cloud.downloadObjectFromCloudStorage(schemaFile, localSchemaFile, bucket);
		
		// uncompress if compressed
		if(GzipUtils.isCompressedFilename(schemaFile)) {
			localSchemaFile = GzipUtils.getUncompressedFilename(localSchemaFile);
		}

		Map<String, Set<Triple>> allSchemaTriples = 
				TripleUtils.getRelevantSchemaTriples(localSchemaFile, TermUtils.RDFS_TBOX);
		
		// get all the triples we care about
		Map<String, Set<Triple>> schemaTriples = new HashMap<>();
		
		for(String term: terms) {
			if(allSchemaTriples.containsKey(term)) {
				schemaTriples.put(term, allSchemaTriples.get(term));
			}
		}
		
		for(Entry<String, Set<Triple>> entry: schemaTriples.entrySet()) {
			log.info("Term: " + entry.getKey());
			log.info("Triples: " + Joiner.on('\n').join(entry.getValue()));
			Set<String> select = GenericRule.getSelect(entry.getValue());
			log.info("\nQuery: " + GenericRule.getQuery(entry.getValue(), "my-table"));
			log.info("Select: " + select);
			log.info("------------------------------------------------------------------------------------------------------------");
		}
		
		
		String query = //"SELECT TOP( title, 10) as title, COUNT(*) as revision_count "
		        //+ "FROM [publicdata:samples.wikipedia] WHERE wp_namespace = 0;";
			 "select subject from swetodlp.swetodlp_triple where " +
			 "object = \"<http://lsdis.cs.uga.edu/projects/semdis/opus#Article_in_Proceedings>\";";
		
		String jobId = this.cloud.startBigDataQuery(query);
		String filename = Utils.TEMP_FOLDER + 
				Utils.encodeFilename("<http://lsdis.cs.uga.edu/projects/semdis/opus#Article_in_Proceedings>") + 
				Constants.DOT_TERMS;
		
		BigInteger rows = this.cloud.saveBigQueryResultsToFile(jobId, filename);
		System.err.println(rows);
		
		String term = "<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter>";
		schemaTriples = new HashMap<>();
		Set<Triple> triples = new HashSet<Triple>();
		/*
		 * 
		Triple [<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter> <http://www.w3.org/2000/01/rdf-schema#domain> <http://lsdis.cs.uga.edu/projects/semdis/opus#Book_Chapter>], inferred=false
		Triple [<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://www.w3.org/2002/07/owl#topDataProperty>], inferred=false
		Triple [<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2001/XMLSchema#string>], inferred=false
		 */
		triples.add(new Triple(term, "<http://www.w3.org/2000/01/rdf-schema#domain>", "<http://lsdis.cs.uga.edu/projects/semdis/opus#Book_Chapter>"));
		triples.add(new Triple(term, "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>", "<http://www.w3.org/2002/07/owl#topDataProperty>"));
		triples.add(new Triple(term, "<http://www.w3.org/2000/01/rdf-schema#range>", "<http://www.w3.org/2001/XMLSchema#string>"));
		schemaTriples.put(term, triples);
		
		Set<Triple> instanceTriples = new HashSet<>();
		instanceTriples.add(new Triple("<http://dblp.uni-trier.de/rec/bibtex/books/kl/snodgrass95/KlineSL95>", term, "\"21\"^^<http://www.w3.org/2001/XMLSchema#integer>"));
		instanceTriples.add(new Triple("<http://dblp.uni-trier.de/rec/bibtex/books/kl/snodgrass95/SooJS95>", term, "\"27\"^^<http://www.w3.org/2001/XMLSchema#integer>"));
		
		Set<Triple> inferredTriples = new HashSet<>();
		
		for(Entry<String, Set<Triple>> entry: schemaTriples.entrySet()) {
			
			term = entry.getKey();
			System.out.println("Reasoning for term: " + term);
			triples = entry.getValue();
			
			// loop through the instance triples probably stored in a file and generate all the triples matching the schema triples set
			for(Triple instanceTriple: instanceTriples) {	
				for(Triple schemaTriple: triples) {
					Rule rule = GenericRule.getRule(schemaTriple);
					inferredTriples.add(rule.head(schemaTriple, instanceTriple));
				}
			}
		}
		System.out.println("\nInferred Triples");
		System.out.println(Joiner.on('\n').join(inferredTriples));
		
		List<String> jobIds = this.cloud.loadLocalFilesIntoBigData(
				Arrays.asList("/Users/omerio/Downloads/umbel_links.nt_out.gz", 
						"/Users/omerio/Downloads/linkedgeodata_links.nt_out.gz"), "swetodlp.test", false);

	}
	
}

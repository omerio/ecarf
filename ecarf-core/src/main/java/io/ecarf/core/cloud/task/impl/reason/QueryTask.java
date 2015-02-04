package io.ecarf.core.cloud.task.impl.reason;

import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.reason.rulebased.GenericRule;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

/**
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class QueryTask implements Callable<Void> {
	
	private final static Logger log = Logger.getLogger(QueryTask.class.getName()); 

	private Term term;
	private CloudService cloud;
	private String decoratedTable;
	private Set<Triple> triples;

	public QueryTask(Term term, Set<Triple> triples, String decoratedTable, CloudService cloud) {
		super();
		this.term = term;
		this.triples = triples;
		this.decoratedTable = decoratedTable;
		this.cloud = cloud;
	}

	@Override
	public Void call() throws Exception {
		// add table decoration to table name
		String query = GenericRule.getQuery(triples, decoratedTable);	

		log.info(this + "\nQuery: " + query);

		String jobId = this.cloud.startBigDataQuery(query);
		String encodedTerm = Utils.encodeFilename(term.getTerm());
		String filename = Utils.TEMP_FOLDER + encodedTerm + Constants.DOT_TERMS;

		// remember the filename and the jobId for this query
		term.setFilename(filename).setJobId(jobId).setEncodedTerm(encodedTerm);

		return null;
	}

}
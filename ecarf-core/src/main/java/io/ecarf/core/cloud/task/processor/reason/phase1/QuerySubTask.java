package io.ecarf.core.cloud.task.processor.reason.phase1;

import io.cloudex.framework.cloud.api.CloudService;
import io.cloudex.framework.utils.FileUtils;
import io.ecarf.core.cloud.task.processor.reason.Term;
import io.ecarf.core.reason.rulebased.GenericRule;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class QuerySubTask implements Callable<Void> {
	 
	private final static Log log = LogFactory.getLog(QuerySubTask.class);

	private Term term;
	private CloudService cloud;
	private String decoratedTable;
	private Set<Triple> triples;

	public QuerySubTask(Term term, Set<Triple> triples, String decoratedTable, CloudService cloud) {
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

		log.info("Query: " + query);

		String jobId = this.cloud.startBigDataQuery(query);
		String encodedTerm = FileUtils.encodeFilename(term.getTerm());
		String filename = Utils.TEMP_FOLDER + encodedTerm + Constants.DOT_TERMS;

		// remember the filename and the jobId for this query
		term.setFilename(filename).setJobId(jobId).setEncodedTerm(encodedTerm);

		return null;
	}

}
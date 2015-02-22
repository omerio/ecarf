package io.ecarf.core.cloud.task.impl.reason;

import java.math.BigInteger;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * Term details used during the querying and reasoning process
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Term {

	private String term;

	private String filename;

	private String jobId;

	private String encodedTerm;
	
	private BigInteger rows = BigInteger.ZERO;
	
	private Long bytes = 0L;


	/**
	 * @param term
	 */
	public Term(String term) {
		super();
		this.term = term;
	}

	/**
	 * @return the term
	 */
	public String getTerm() {
		return term;
	}

	/**
	 * @param term the term to set
	 */
	public Term setTerm(String term) {
		this.term = term;
		return this;
	}

	/**
	 * @return the filename
	 */
	public String getFilename() {
		return filename;
	}

	/**
	 * @param filename the filename to set
	 */
	public Term setFilename(String filename) {
		this.filename = filename;
		return this;
	}

	/**
	 * @return the jobId
	 */
	public String getJobId() {
		return jobId;
	}

	/**
	 * @param jobId the jobId to set
	 */
	public Term setJobId(String jobId) {
		this.jobId = jobId;
		return this;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

	/**
	 * @return the encodedTerm
	 */
	public String getEncodedTerm() {
		return encodedTerm;
	}

	/**
	 * @param encodedTerm the encodedTerm to set
	 */
	public void setEncodedTerm(String encodedTerm) {
		this.encodedTerm = encodedTerm;
	}

	/**
	 * @return the rows
	 */
	public BigInteger getRows() {
		return rows;
	}

	/**
	 * @param rows the rows to set
	 */
	public Term setRows(BigInteger rows) {
		this.rows = rows;
		return this;
	}
	
	/**
	 * @return the bytes
	 */
	public Long getBytes() {
		return bytes;
	}

	/**
	 * @param bytes the bytes to set
	 */
	public Term setBytes(Long bytes) {
		this.bytes = bytes;
		return this;
	}

}
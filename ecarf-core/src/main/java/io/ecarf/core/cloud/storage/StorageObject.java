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
package io.ecarf.core.cloud.storage;

import io.ecarf.core.utils.Constants;

import java.math.BigInteger;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * Represent a generic mass cloud storage object
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class StorageObject {
	
	private String name;
	
	private BigInteger size;
	
	private String contentType;
	
	private String directLink;
	
	private String uri;

	/**
	 * check if this object is compressed
	 * @return
	 */
	public boolean isCompressed() {
		return Constants.GZIP_CONTENT_TYPE.equals(this.contentType);
	}
	
	
	
	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the size
	 */
	public BigInteger getSize() {
		return size;
	}

	/**
	 * @param size the size to set
	 */
	public void setSize(BigInteger size) {
		this.size = size;
	}

	/**
	 * @return the contentType
	 */
	public String getContentType() {
		return contentType;
	}

	/**
	 * @param contentType the contentType to set
	 */
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	/**
	 * @return the directLink
	 */
	public String getDirectLink() {
		return directLink;
	}

	/**
	 * @param directLink the directLink to set
	 */
	public void setDirectLink(String directLink) {
		this.directLink = directLink;
	}



	/**
	 * @return the uri
	 */
	public String getUri() {
		return uri;
	}

	/**
	 * @param uri the uri to set
	 */
	public void setUri(String uri) {
		this.uri = uri;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}

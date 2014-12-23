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
package io.ecarf.core.exceptions;

import java.io.IOException;

/**
 * A representation of an exception thrown by the processing node
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class NodeException extends IOException {
	
	private static final long serialVersionUID = -3227912708786931598L;
	
	/**
	 * The id of the instance (VM) that has thrown this exception
	 */
	private String nodeId;

	/**
	 * 
	 */
	public NodeException() {
		
	}

	/**
	 * @param message
	 */
	public NodeException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public NodeException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public NodeException(String message, Throwable cause) {
		super(message, cause);
	}
	
	/**
	 * @param message
	 * @param cause
	 */
	public NodeException(String message, Throwable cause, String nodeId) {
		super(message, cause);
		this.nodeId = nodeId;
	}

	/**
	 * @return the nodeId
	 */
	public String getNodeId() {
		return nodeId;
	}

	/**
	 * @param nodeId the nodeId to set
	 */
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}


}

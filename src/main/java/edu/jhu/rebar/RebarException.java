/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


/**
 * Created on Apr 13, 2012.
 */
package edu.jhu.rebar;

/**
 * @author thomamj1
 *
 */
public class RebarException extends Exception {

	/**
	 * Eclipse-generated
	 */
	private static final long serialVersionUID = 168024806017512250L;

	/**
	 * @param message
	 */
	public RebarException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public RebarException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public RebarException(String message, Throwable cause) {
		super(message, cause);
	}
}

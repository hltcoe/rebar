/*
 * Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
 * See LICENSE in the project root directory.
 */

package edu.jhu.hlt.rebar.client.iterators;

import java.util.Iterator;

/**
 * @author max
 *
 */
public interface AutoCloseableIterator<E> extends Iterator<E>, AutoCloseable {

}

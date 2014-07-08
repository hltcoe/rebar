/*
 * Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
 * See LICENSE in the project root directory.
 */

package edu.jhu.hlt.rebar.client.iterators;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * @author max
 *
 */
public abstract class AbstractAccumuloIterator<E> implements AutoCloseableIterator<E> {

  private final ScannerBase sc;
  protected final Iterator<Entry<Key, Value>> iter; 
  private boolean closed = false;
  
  /**
   * 
   */
  public AbstractAccumuloIterator(ScannerBase sc) {
    this.sc = sc;
    this.iter = sc.iterator();
  }
  
  public AbstractAccumuloIterator() {
    this.sc = null;
    this.iter = null;
    this.closed = true;
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#hasNext()
   */
  @Override
  public final boolean hasNext() {
    if (this.closed)
      return false;
    else
      return this.iter.hasNext();
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#remove()
   */
  @Override
  public final void remove() {
    throw new UnsupportedOperationException("You cannot remove with this iterator.");
  }

  /* (non-Javadoc)
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public final void close() throws Exception {
    if (!this.closed) {
      this.sc.close();
      this.closed = true;
    }
  }
}

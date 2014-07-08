/*
 * Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
 * See LICENSE in the project root directory.
 */

package edu.jhu.hlt.rebar.client.iterators;

import org.apache.accumulo.core.client.ScannerBase;

import edu.jhu.hlt.concrete.util.Serialization;

/**
 * @author max
 *
 */
public abstract class AbstractAutoCloseableThriftIterator<T> extends AbstractAccumuloIterator<T> {

  protected final Serialization ser;
  
  /**
   * @param sc
   */
  public AbstractAutoCloseableThriftIterator(ScannerBase sc) {
    super(sc);
    this.ser = new Serialization();
  }

  /**
   * 
   */
  public AbstractAutoCloseableThriftIterator() {
    super();
    this.ser = new Serialization();
  }
}

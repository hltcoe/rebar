/*
 * Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
 * See LICENSE in the project root directory.
 */

package edu.jhu.hlt.rebar.client.iterators;

import org.apache.accumulo.core.client.ScannerBase;

import com.esotericsoftware.kryo.Kryo;

/**
 * @author max
 *
 */
public abstract class KryoIterator<E> extends AbstractAccumuloIterator<E> {
  protected final Kryo kryo;
  
  public KryoIterator(ScannerBase sc) {
    super(sc);
    this.kryo = new Kryo();
  }
}

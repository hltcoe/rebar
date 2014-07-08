/*
 * 
 */
package edu.jhu.hlt.rebar.client.iterators;

import java.util.NoSuchElementException;

/**
 * Empty iterator.
 * 
 * @author max
 */
public class EmptyAutoCloseableAccumuloIterator<T> extends AbstractAccumuloIterator<T> {

  /**
   * 
   */
  public EmptyAutoCloseableAccumuloIterator() {
    super();
  }

  @Override
  public T next() {
    throw new NoSuchElementException("This iterator is empty.");
  }
}

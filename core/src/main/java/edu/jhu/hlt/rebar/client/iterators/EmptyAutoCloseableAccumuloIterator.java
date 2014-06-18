/**
 * 
 */
package edu.jhu.hlt.rebar.client.iterators;

import java.util.NoSuchElementException;

/**
 * Empty iterator.
 * 
 * @author max
 */
public class EmptyAutoCloseableAccumuloIterator<T> extends AbstractThriftIterator<T> {

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

  /* (non-Javadoc)
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public void close() {
    // nothing.
  }
}

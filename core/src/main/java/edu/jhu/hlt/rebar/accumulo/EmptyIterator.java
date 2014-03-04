/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Iterator;

/**
 * Empty iterator.
 * 
 * @author max
 */
public class EmptyIterator<T> implements Iterator<T> {

  /**
   * 
   */
  public EmptyIterator() {

  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public T next() {
    throw new UnsupportedOperationException("This iterator is empty.");
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("You cannot remove with this iterator.");
  }

}

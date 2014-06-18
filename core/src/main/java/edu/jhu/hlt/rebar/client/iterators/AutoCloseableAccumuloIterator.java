/**
 * 
 */
package edu.jhu.hlt.rebar.client.iterators;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * @author max
 *
 */
abstract class AutoCloseableAccumuloIterator<E> implements Iterator<E>, AutoCloseable {
  protected final Iterator<Entry<Key, Value>> iter;
  
  /**
   * 
   */
  public AutoCloseableAccumuloIterator(Iterator<Entry<Key, Value>> iter) {
    this.iter = iter;
  }
}

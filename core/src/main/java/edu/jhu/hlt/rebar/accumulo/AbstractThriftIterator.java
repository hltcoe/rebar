/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 * 
 * TODO: Better type bounding ON T.
 */
public abstract class AbstractThriftIterator<T> implements Iterator<T> {

  protected final Iterator<Entry<Key, Value>> iter;
  protected final TDeserializer deser = new TDeserializer(new TBinaryProtocol.Factory());
  
  /**
   * 
   */
  public AbstractThriftIterator(Iterator<Entry<Key, Value>> iter) {
    this.iter = iter;
  }

  @Override
  public final boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public abstract T next();

  @Override
  public final void remove() {
    throw new UnsupportedOperationException("You cannot remove with this iterator.");
  }
}

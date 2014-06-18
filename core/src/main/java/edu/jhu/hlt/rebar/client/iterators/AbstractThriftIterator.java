/**
 * 
 */
package edu.jhu.hlt.rebar.client.iterators;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;

/**
 * @author max
 * 
 * TODO: Better type bounding ON T.
 */
public abstract class AbstractThriftIterator<T> implements Iterator<T>, AutoCloseable {

  private final ScannerBase sc;
  protected final Iterator<Entry<Key, Value>> iter; 
  protected final TDeserializer deser = new TDeserializer(new TBinaryProtocol.Factory());
  private boolean closed = false;
  
  /**
   * 
   */
  public AbstractThriftIterator(ScannerBase sc) {
    this.sc = sc;
    this.iter = sc.iterator();
  }
  
  // mega hack to allow for empty iterator.
  protected AbstractThriftIterator() {
    this.closed = true;
    this.sc = null;
    this.iter = null;
  }

  @Override
  public final boolean hasNext() {
    // if closed, return false.
    if (this.closed)
      return false;
    
    boolean result = this.iter.hasNext();
    if (!result)
      this.close();
    
    return result;
  }

  @Override
  public abstract T next();
  
  @Override
  public void close() {
    if (!closed) {
      this.sc.close();
      this.closed = true;
    }
  }
  
  /* (non-Javadoc)
   * @see java.util.Iterator#remove()
   */
  @Override
  public final void remove() {
    throw new UnsupportedOperationException("You cannot remove with this iterator.");
  }
}

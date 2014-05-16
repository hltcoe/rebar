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

  protected final ScannerBase sc;
  protected final Iterator<Entry<Key, Value>> iter;
  protected final TDeserializer deser = new TDeserializer(new TBinaryProtocol.Factory());
  protected boolean closed = false;
  
  /**
   * 
   */
  public AbstractThriftIterator(ScannerBase sc) {
    this.sc = sc;
    this.iter = sc.iterator();
  }

  @Override
  public final boolean hasNext() {
    // if closed, return false.
    if (this.closed)
      return false;
    
    boolean result = this.iter.hasNext();
    if (!result) {
      this.close();
      this.closed = true;
    }
    
    return result;
  }

  @Override
  public abstract T next();

  @Override
  public final void remove() {
    throw new UnsupportedOperationException("You cannot remove with this iterator.");
  }
  
  @Override
  public void close() {
    this.sc.close();
  }
}

/**
 * 
 */
package edu.jhu.hlt.rebar.client.iterators;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;

/**
 * @author max
 * 
 * TODO: Better type bounding ON T.
 */
public abstract class AbstractThriftIterator<T> extends AutoCloseableAccumuloIterator<T> {

  protected final ScannerBase sc;
  protected final TDeserializer deser = new TDeserializer(new TBinaryProtocol.Factory());
  private boolean closed = false;
  
  /**
   * 
   */
  public AbstractThriftIterator(ScannerBase sc) {
    super(sc.iterator());
    this.sc = sc;
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
}

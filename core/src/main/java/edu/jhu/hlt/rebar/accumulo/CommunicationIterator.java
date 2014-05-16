/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.client.iterators.AbstractThriftIterator;

/**
 * @author max
 *
 */
public class CommunicationIterator extends AbstractThriftIterator<Communication> {

  /**
   * @param iter
   */
  public CommunicationIterator(ScannerBase sc) {
    super(sc);
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.accumulo.AbstractThriftIterator#next()
   */
  @Override
  public Communication next() {
    try {
      Communication c = new Communication();
      Entry<Key, Value> entry = this.iter.next();
      deser.deserialize(c, entry.getValue().get());
      return c;
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }
}

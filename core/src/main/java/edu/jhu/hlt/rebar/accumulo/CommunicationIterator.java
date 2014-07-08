/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.util.ConcreteException;
import edu.jhu.hlt.rebar.client.iterators.AbstractAutoCloseableThriftIterator;

/**
 * @author max
 *
 */
public class CommunicationIterator extends AbstractAutoCloseableThriftIterator<Communication> {

  /**
   * 
   * @param sc
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
      this.ser.fromBytes(c, entry.getValue().get());
      return c;
    } catch (ConcreteException e) {
      throw new RuntimeException(e);
    }
  }
}

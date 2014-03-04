/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;

import edu.jhu.hlt.concrete.Communication;

/**
 * @author max
 */
public class CommunicationIterator extends AbstractThriftIterator<Communication> {
  
  /**
   * 
   */
  public CommunicationIterator(Iterator<Entry<Key, Value>> iter) {
    super(iter);
  }

  @Override
  public Communication next() {
    try {
      Communication c = new Communication();
      deser.deserialize(c, iter.next().getValue().get());
      return c;
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }
}

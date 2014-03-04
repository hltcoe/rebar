/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import edu.jhu.hlt.concrete.Communication;

/**
 * Tiny iterator wrapper for API usage.
 * 
 * @author max
 */
public class CommunicationIterator implements Iterator<Communication> {

  private final Iterator<Entry<Key, Value>> iter;
  private final TDeserializer deser = new TDeserializer(new TBinaryProtocol.Factory());
  
  /**
   * 
   */
  public CommunicationIterator(Iterator<Entry<Key, Value>> iter) {
    this.iter = iter;
  }

  @Override
  public boolean hasNext() {
    return this.iter.hasNext();
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

  @Override
  public void remove() {
    throw new UnsupportedOperationException("You cannot remove with this iterator.");
  }
}

/**
 * 
 */
package edu.jhu.hlt.rebar.client.iterators;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public abstract class WholeRowMergingIterator<K> extends AbstractThriftIterator<Communication> {

  protected final String stageName;
  
  /**
   * @param iter
   */
  public WholeRowMergingIterator(Iterator<Entry<Key, Value>> iter, String stageName) {
    super(iter);
    this.stageName = stageName;
  }

  protected abstract K extractInterestingKFromWholeRow(Map<Key, Value> wholeRowMap, String colQ) throws RebarException, TException;
  
  /**
   * Takes a row via {@link WholeRowIterator#decodeRow(Key, Value)} as input. Attempts
   * to find the {@link Communication} object in the row. 
   * 
   * @param decodedRowViaWRI - via {@link WholeRowIterator#decodeRow(Key, Value)}
   * @return a {@link Communication} if found
   * @throws TException - if there is a deserialization error
   * @throws RebarException - if there is no {@link Communication} in this row
   */
  protected Communication getCommFromColumnFamily(Map<Key, Value> decodedRowViaWRI) throws TException, RebarException {
    Communication d = null;
    Iterator<Entry<Key, Value>> iter = decodedRowViaWRI.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<Key, Value> entry = iter.next();
      Key k = entry.getKey();
      if (k.compareColumnFamily(new Text(Constants.DOCUMENT_COLF)) == 0) {
        d = new Communication();
        new TDeserializer(new TBinaryProtocol.Factory()).deserialize(d, entry.getValue().get());
        iter.remove();
        decodedRowViaWRI.remove(k);
      }
    }
    
    if (d == null)
      throw new RebarException("Did not find a root communication in this row.");

    return d;
  }
}

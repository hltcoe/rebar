/**
 * 
 */
package edu.jhu.hlt.rebar.client.iterators;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TBinaryProtocol;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * Bloated code written due to Java's lack of functions-as-first-class-citizenry.
 * 
 * Supports implementations of {@link AbstractThriftIterator}.
 * 
 * @author max
 */
public class ThriftRowExtractor {

  /**
   * Silly enum to support column family or column qualifier based matching.
   * 
   * @author max
   */
  private enum ColType {
    CF {
      @Override
      public int compare(Key k, String text) {
        return k.compareColumnFamily(new Text(text));
      }
    },
    CQ {
      @Override
      public int compare(Key k, String text) {
        return k.compareColumnQualifier(new Text(text));
      }
    };

    public abstract int compare(Key k, String text);
  }

  private final Map<Key, Value> wholeRowMap;

  /**
   * 
   */
  public ThriftRowExtractor(Map<Key, Value> wholeRowMap) {
    this.wholeRowMap = wholeRowMap;
  }

  /**
   * Attempts to find the {@link T}, aka a 'thrift-like object', in the row.
   * 
   * @return a {@link T} if found
   * @throws RebarException
   *           - if there is no {@link T} in this row, or if there was an error during serialization
   */
  public <T extends TBase<T, ? extends TFieldIdEnum>> T extract(T object, String stageName) throws RebarException {
    try {
      byte[] bytez = this.extractBytes(stageName, ColType.CQ);
      new TDeserializer(new TBinaryProtocol.Factory()).deserialize(object, bytez);
      return object;
    } catch (TException e) {
      throw new RebarException(e);
    }
  }

  /**
   * Attempts to find the {@link Communication} object in the row.
   * 
   * @return a {@link Communication} if found
   * @throws RebarException
   *           if there is no {@link Communication} in this row, or if there was an error during serialization
   */
  public Communication extractCommunication() throws RebarException {
    try {
      Communication d = new Communication();
      byte[] bytez = this.extractBytes(Constants.DOCUMENT_COLF, ColType.CF);
      new TDeserializer(new TBinaryProtocol.Factory()).deserialize(d, bytez);
      return d;
    } catch (TException e) {
      throw new RebarException(e);
    }
  }

  /**
   * Find the bytes from this row; mutate the iterator so that the row does not appear
   * in future iterations. 
   * 
   * @param colStr - string of the ColF / ColQ to match
   * @param ct - the {@link ColType} to match on
   * @return an array of bytes with the {@link Value} of the row matched
   * @throws RebarException if nothing is found
   */
  private byte[] extractBytes(String colStr, ColType ct) throws RebarException {
    byte[] bytez = null;
    Iterator<Entry<Key, Value>> iter = this.wholeRowMap.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<Key, Value> entry = iter.next();
      Key k = entry.getKey();
      if (ct.compare(k, colStr) == 0) {
        bytez = entry.getValue().get();
        iter.remove();
        this.wholeRowMap.remove(k);
      }
    }

    if (bytez == null)
      throw new RebarException("Did not find anything matching " + ct.toString() + ": " + colStr);

    return bytez;
  }
}

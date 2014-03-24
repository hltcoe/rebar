/**
 * 
 */
package edu.jhu.hlt.rebar.client.iterators;

import java.io.IOException;
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
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.rebar.RebarException;

/**
 * An {@link Iterator} that will merge {@link SectionSegmentation}s to 
 * {@link Communication} objects.
 * 
 * @author max
 */
public class SectionMergingIterator extends WholeRowMergingIterator<SectionSegmentation> {
  
  /**
   * @param iter
   * @param stageName
   */
  public SectionMergingIterator(Iterator<Entry<Key, Value>> iter, String stageName) {
    super(iter, stageName);
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.accumulo.AbstractThriftIterator#next()
   */
  @Override
  public Communication next() {
    try {
      Entry<Key, Value> e = this.iter.next();
      Map<Key, Value> rows = WholeRowIterator.decodeRow(e.getKey(), e.getValue());
      // NOTE: ROWS is mutated by below calls
      Communication root = getCommFromColumnFamily(rows);
      SectionSegmentation ss = extractInterestingKFromWholeRow(rows, this.stageName);
      root.addToSectionSegmentations(ss);
      return root;
    } catch (IOException | TException | RebarException e) {
      throw new RuntimeException(e);
    }  
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.accumulo.WholeRowMergingIterator#extractInterestingTFromWholeRow(java.util.Map, java.lang.String)
   */
  @Override
  protected SectionSegmentation extractInterestingKFromWholeRow(Map<Key, Value> wholeRowMap, String colQ) throws RebarException, TException {
    SectionSegmentation d = null;
    Iterator<Entry<Key, Value>> iter = wholeRowMap.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<Key, Value> entry = iter.next();
      Key k = entry.getKey();
      if (k.compareColumnQualifier(new Text(colQ)) == 0) {
        d = new SectionSegmentation();
        new TDeserializer(new TBinaryProtocol.Factory()).deserialize(d, entry.getValue().get());
        iter.remove();
        wholeRowMap.remove(k);
      }
    }
    
    if (d == null)
      throw new RebarException("Did not find a section segmentation in this row.");

    return d;
  }
}

/**
 * 
 */
package edu.jhu.hlt.rebar.client.iterators;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.rebar.RebarException;

/**
 * An {@link Iterator} that will merge {@link SectionSegmentation}s to 
 * {@link Communication} objects.
 * 
 * @author max
 */
public class SectionMergingIterator extends AbstractAccumuloIterator<Communication> {
  
  protected final String stageName;
  
  /**
   * 
   * @param sc
   * @param stageName
   */
  public SectionMergingIterator(ScannerBase sc, String stageName) {
    super(sc);
    this.stageName = stageName;
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
      ThriftRowExtractor ext = new ThriftRowExtractor(rows);
      Communication root = ext.extractCommunication();
      SectionSegmentation ss = ext.extract(new SectionSegmentation(), this.stageName);
      this.merge(root, ss);
      return root;
    } catch (IOException | RebarException e) {
      throw new RuntimeException(e);
    }  
  }
  
  /**
   * NOTE: root is mutated by the following method.
   * 
   * @param root
   * @param ss
   */
  protected void merge(Communication root, SectionSegmentation ss) {
    root.addToSectionSegmentations(ss);
  }
}

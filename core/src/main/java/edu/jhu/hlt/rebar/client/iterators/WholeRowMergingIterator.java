/**
 * 
 */
package edu.jhu.hlt.rebar.client.iterators;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import edu.jhu.hlt.concrete.Communication;

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
}

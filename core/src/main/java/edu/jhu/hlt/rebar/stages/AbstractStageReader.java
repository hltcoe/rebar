/**
 * 
 */
package edu.jhu.hlt.rebar.stages;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.accumulo.AbstractReader;

/**
 * @author max
 *
 */
public abstract class AbstractStageReader<T> extends AbstractReader<T> {

  /**
   * @throws RebarException 
   * 
   */
  public AbstractStageReader() throws RebarException {
    this(Constants.getConnector());
  }
  
  public AbstractStageReader(Connector conn) throws RebarException {
    super(conn, Constants.DOCUMENT_TABLE_NAME, Constants.DOCUMENT_IDX_TABLE);
  }
  
  protected Iterator<T> mergedIterator(String stageName) throws RebarException {
    Range r = new Range("stage:"+stageName);
    Set<Range> ranges = this.scanIndexTableColF(r);
    Iterator<Entry<Key, Value>> eIter = this.batchScanMainTableWholeRowIterator(ranges);
    return this.accumuloIterToTIter(eIter);
  }
}

/**
 * 
 */
package edu.jhu.hlt.rebar.stage;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.accumulo.AbstractCommunicationReader;

/**
 * @author max
 *
 */
public abstract class AbstractStageReader extends AbstractCommunicationReader {

  protected final String stageName;
  
  /**
   * @throws RebarException 
   * 
   */
  public AbstractStageReader(String stageName) throws RebarException {
    this(Constants.getConnector(), stageName);
  }
  
  public AbstractStageReader(Connector conn, String stageName) throws RebarException {
    super(conn);
    if (new StageReader().exists(stageName))
      this.stageName = stageName;
    else
      throw new RebarException("Stage: " + stageName + " doesn't exist; you need to create it first.");
  }
  
  protected Iterator<Communication> mergedIterator() throws RebarException {
    Range r = new Range("stage:"+this.stageName);
    Set<Range> ranges = this.scanIndexTableColF(r);
    Iterator<Entry<Key, Value>> eIter = this.batchScanMainTableWholeRowIterator(ranges);
    return this.accumuloIterToTIter(eIter);
  }
  
  public final Iterator<Communication> getAll() throws RebarException {
    return this.mergedIterator();
  }
}

/**
 * 
 */
package edu.jhu.hlt.rebar.stage;

import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Range;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.accumulo.AbstractCommunicationReader;
import edu.jhu.hlt.rebar.client.iterators.AutoCloseableAccumuloIterator;

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
    if (new StageReader(this.conn).exists(stageName))
      this.stageName = stageName;
    else
      throw new RebarException("Stage: " + stageName + " doesn't exist; you need to create it first.");
  }
  
  protected AutoCloseableAccumuloIterator<Communication> mergedIterator() throws RebarException {
    Range r = new Range("stage:"+this.stageName);
    Set<Range> ranges = this.scanIndexTableColF(r);
    BatchScanner eIter = this.batchScanMainTableWholeRowIterator(ranges);
    return this.accumuloIterToTIter(eIter);
  }
  
  public final AutoCloseableAccumuloIterator<Communication> getAll() throws RebarException {
    return this.mergedIterator();
  }
}

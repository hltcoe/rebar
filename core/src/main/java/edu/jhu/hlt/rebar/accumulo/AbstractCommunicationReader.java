/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Collection;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.client.iterators.AbstractAccumuloIterator;

/**
 * @author max
 *
 */
public abstract class AbstractCommunicationReader extends AbstractReader<Communication> {
  
  /**
   * @throws RebarException
   */
  public AbstractCommunicationReader() throws RebarException {
    this(Constants.getConnector());
  }

  /**
   * @param conn
   * @throws RebarException
   */
  public AbstractCommunicationReader(Connector conn) throws RebarException {
    super(conn, Constants.DOCUMENT_TABLE_NAME, Constants.DOCUMENT_IDX_TABLE);
  }
  
  @Override
  protected AbstractAccumuloIterator<Communication> accumuloIterToTIter(ScannerBase sc) throws RebarException {
    return new CommunicationIterator(sc);
  }
  
  @Override
  protected BatchScanner batchScanMainTable(Collection<Range> ids) throws RebarException {
    BatchScanner docsc = null;
    try {
      docsc = this.conn.createBatchScanner(this.tableName, Configuration.getAuths(), 8);
      docsc.setRanges(ids);
      docsc.fetchColumnFamily(new Text(Constants.DOCUMENT_COLF));
      return docsc;
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }
}

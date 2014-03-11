/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

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
  protected Iterator<Communication> accumuloIterToTIter(Iterator<Entry<Key, Value>> accIter) throws RebarException {
    return new CommunicationIterator(accIter);
  }
  
  @Override
  protected Iterator<Entry<Key, Value>> batchScanMainTable(Collection<Range> ids) throws RebarException {
    BatchScanner docsc = null;
    try {
      docsc = this.conn.createBatchScanner(this.tableName, Configuration.getAuths(), 8);
      docsc.setRanges(ids);
      docsc.fetchColumnFamily(new Text(Constants.DOCUMENT_COLF));
      return docsc.iterator();
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    } finally {
      if (docsc != null)
        docsc.close();
    }
  }
}

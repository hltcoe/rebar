/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;

import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public abstract class AbstractCommunicationWriter extends AbstractAccumuloClient implements AutoCloseable {

  protected BatchWriter bw;  
  
  /**
   * @throws RebarException
   */
  public AbstractCommunicationWriter() throws RebarException {
    this(Constants.getConnector());
  }

  /**
   * @param conn
   * @throws RebarException
   */
  public AbstractCommunicationWriter(Connector conn) throws RebarException {
    super(conn);
    this.tableOps.createTableIfNotExists(Constants.DOCUMENT_TABLE_NAME);
    try {
      this.bw = this.conn.createBatchWriter(Constants.DOCUMENT_TABLE_NAME, defaultBwOpts.getBatchWriterConfig());
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }

  /* (non-Javadoc)
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public void close() throws Exception {
    this.bw.close();
  }
}

/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;

import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public class AbstractAccumuloWriter extends AbstractAccumuloClient implements AutoCloseable {

  protected final String tableName;
  protected final String idxTableName;
  protected final BatchWriter bw;
  protected final BatchWriter idxBw;
  
  /**
   * @throws RebarException
   */
  public AbstractAccumuloWriter(String tableName, String idxTableName) throws RebarException {
    this(Constants.getConnector(), tableName, idxTableName);
  }

  /**
   * @param conn
   * @throws RebarException
   */
  public AbstractAccumuloWriter(Connector conn, String tableName, String idxTableName) throws RebarException {
    super(conn);
    this.tableName = tableName;
    this.idxTableName = idxTableName;
    this.bw = this.safeBatchWriter(this.tableName);
    this.idxBw = this.safeBatchWriter(this.idxTableName);
  }

  /* (non-Javadoc)
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public void close() throws RebarException {
    try {
      this.idxBw.close();
      this.bw.close();
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    }
  }
  
}

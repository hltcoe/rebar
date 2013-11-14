/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.thrift.TException;

import edu.jhu.hlt.rebar.RebarException;

import com.maxjthomas.dumpster.Document;
import com.maxjthomas.dumpster.Reader;
import com.maxjthomas.dumpster.Stage;

/**
 * @author max
 *
 */
public class RebarReader extends AbstractAccumuloClient implements Reader.Iface {

  private final AccumuloStageHandler ash;
  
  /**
   * @throws RebarException
   */
  public RebarReader() throws RebarException {
    this(getConnector());
  }

  /**
   * @param conn
   * @throws RebarException
   */
  public RebarReader(Connector conn) throws RebarException {
    super(conn);
    this.ash = new AccumuloStageHandler(this.conn);
  }

  /* (non-Javadoc)
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public void close() throws Exception {
    this.ash.close();
    this.bw.close();
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.accumulo.AbstractAccumuloClient#flush()
   */
  @Override
  public void flush() throws RebarException {
    this.ash.flush();
    try {
      this.bw.flush();
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    }
  }

  /* (non-Javadoc)
   * @see com.maxjthomas.dumpster.Reader.Iface#getAnnotatedDocuments(com.maxjthomas.dumpster.Stage)
   */
  @Override
  public Set<Document> getAnnotatedDocuments(Stage stage) throws TException {
    // TODO Auto-generated method stub
    if (!this.ash.stageExists(stage.name))
      throw new TException("Stage " + stage.name + " doesn't exist; can't get its documents.");
    
    return null;
  }
}

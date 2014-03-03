/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public class CleanIngester extends AbstractIngester {

  private static final Logger logger = LoggerFactory.getLogger(CleanIngester.class);
  
  /**
   * @throws RebarException
   */
  public CleanIngester() throws RebarException {
    this(Constants.getConnector());
  }

  /**
   * @param conn
   * @throws RebarException
   */
  public CleanIngester(Connector conn) throws RebarException {
    super(conn);
    this.tableOps.createTableIfNotExists("doc_idx");
  }
  
  public boolean isDocumentIngested(Communication d) throws RebarException {
    try {
      Scanner sc = this.conn.createScanner("doc_idx", Configuration.getAuths());
      Range r = new Range("doc_id:" + d.id);
      sc.setRange(r);
      return sc.iterator().hasNext();
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }
  
  public void ingest(Communication d) throws RebarException {
    logger.debug("Got ingest request: " + d.id);
    if (isDocumentIngested(d))
      return;
    
    final Mutation m = new Mutation(d.id);
    
    try {
      Value v = new Value(this.serializer.serialize(d));
      m.put(Constants.DOCUMENT_COLF, "", v);
      this.bw.addMutation(m);
    } catch (MutationsRejectedException | TException e) {
      throw new RebarException(e);
    }
  }
}

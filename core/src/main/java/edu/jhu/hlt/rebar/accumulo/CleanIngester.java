/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.communications.SuperCommunication;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;

/**
 * @author max
 *
 */
public class CleanIngester extends AbstractCommunicationWriter {

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
  }
  
  public void close() throws RebarException {
    try {
      this.idxBw.close();
      this.bw.close();
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    }
  }
  
  public void ingestAndStrip(Communication c) throws RebarException {
    SuperCommunication stripped = new SuperCommunication(c).stripAnnotations();
    this.ingest(stripped.getCopy());
  }
  
  public void ingest(Communication d) throws RebarException {
    logger.debug("Got ingest request: " + d.id);
    if (new CommunicationReader().exists(d))
      return;
    
    if (new SuperCommunication(d).containsAnnotations())
      throw new RebarException("There are annotations on communication: " 
          + d.getUuid() + "; cannot ingest. If you want to strip them automatically, call ingestAndStrip().");

    try {
      final Mutation m = new Mutation(d.uuid);
      Value v = new Value(this.serializer.serialize(d));
      m.put(Constants.DOCUMENT_COLF, "", v);
      this.bw.addMutation(m);
      
      this.idxBw.addMutation(Util.generateEmptyValueMutation("doc_id:"+d.id, d.uuid, ""));
      this.idxBw.addMutation(Util.generateEmptyValueMutation("type:"+d.type.toString(), d.uuid, ""));
      
      if (d.startTime != 0)
        this.idxBw.addMutation(Util.generateEmptyValueMutation("date:"+new DateTime(d.startTime * 1000).toString(), d.uuid, ""));
    } catch (MutationsRejectedException | TException e) {
      throw new RebarException(e);
    }
  }
}

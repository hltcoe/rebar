/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.asphalt.services.Ingester;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * 
 * @author max
 *
 */
public class RebarIngester extends AbstractAccumuloClient implements AutoCloseable, Ingester.Iface {

  private static final Logger logger = LoggerFactory.getLogger(RebarIngester.class);
  
  private final Set<String> pendingInserts;
  
  /**
   * @throws RebarException 
   * 
   */
  public RebarIngester() throws RebarException {
    this(Constants.getConnector());
  }
  
  public RebarIngester(Connector conn) throws RebarException {
    super(conn);
    this.pendingInserts = new HashSet<>();
  }
  
  private boolean isDocumentIngested(Communication d) {
    // return existingIds.contains(d.getId());
    return false;
  }
  
  private boolean isDocumentPendingIngest(Communication d) {
    return this.pendingInserts.contains(d.getId());
  }
  
  @Override
  public void flush() throws RebarException {
    try {
      this.bw.flush();
      this.pendingInserts.clear();
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    }
  }
  
  public void close() throws RebarException {
    try {
      this.bw.close();
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    } finally {
      
    }
  }
  
  @Override
  public void ingest(Communication d) throws TException {
    logger.debug("Got ingest request: " + d.id);
    try {
      if (isDocumentIngested(d) || isDocumentPendingIngest(d))
        return;
      
      final Mutation m = new Mutation(d.id);
      
      try {
        Value v = new Value(this.serializer.serialize(d));
        m.put(Constants.DOCUMENT_COLF, "", v);
        this.bw.addMutation(m);
        this.pendingInserts.add(d.getId());
      } catch (MutationsRejectedException | TException e) {
        throw new RebarException(e);
      }
    } catch (RebarException e) {
      throw new TException(e);
    }
  }
}

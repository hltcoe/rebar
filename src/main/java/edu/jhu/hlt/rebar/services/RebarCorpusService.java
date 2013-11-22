/**
 * 
 */
package edu.jhu.hlt.rebar.services;

import org.apache.accumulo.core.client.Connector;

import edu.jhu.hlt.concrete.CorpusHandler;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.accumulo.RebarCorpusHandler;

/**
 * @author max
 *
 */
public class RebarCorpusService extends AbstractThriftServer {
  
  /**
   * @param port
   * @throws RebarException
   */
  public RebarCorpusService(int port) throws RebarException {
    this(Constants.getConnector(), port);
  }
  
  public RebarCorpusService(Connector conn, int port) throws RebarException {
    this(conn, port, new RebarCorpusHandler(conn));
  }

  /**
   * @param port
   * @param processor
   * @throws RebarException
   */
  private RebarCorpusService(Connector conn, int port, RebarCorpusHandler rch) throws RebarException {
    super(port, rch, new CorpusHandler.Processor<>(rch));
  }
}

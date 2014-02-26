/**
 * 
 */
package edu.jhu.hlt.rebar.services;

import org.apache.accumulo.core.client.Connector;

import edu.jhu.hlt.asphalt.services.StageHandler;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.accumulo.RebarStageHandler;

/**
 * @author max
 *
 */
public class RebarStageServer extends AbstractThriftServer {

  /**
   * @param port
   * @throws RebarException
   */
  public RebarStageServer(int port) throws RebarException {
    this(Constants.getConnector(), port);
  }
  
  public RebarStageServer(Connector conn, int port) throws RebarException {
    this(conn, port, new RebarStageHandler(conn));
  }
  
  public RebarStageServer(Connector conn, int port, RebarStageHandler rsh) throws RebarException {
    super(port, rsh, new StageHandler.Processor<>(rsh));
  }
}

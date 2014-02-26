/**
 * 
 */
package edu.jhu.hlt.rebar.services;


import org.apache.accumulo.core.client.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.asphalt.services.Ingester;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.accumulo.RebarIngester;

/**
 * @author max
 *
 */
public class RebarIngesterServer extends AbstractThriftServer {
  
  private static final Logger logger = LoggerFactory.getLogger(RebarIngesterServer.class);
  
  /**
   * @throws RebarException 
   * 
   */
  public RebarIngesterServer(int port) throws RebarException {
    this(Constants.getConnector(), port);
  }
  
  public RebarIngesterServer(Connector conn, int port) throws RebarException {
    this(conn, port, new RebarIngester(conn));
  }
  
  /**
   * @throws RebarException 
   * 
   */
  private RebarIngesterServer(Connector conn, int port, RebarIngester ri) throws RebarException {
    super(port, ri, new Ingester.Processor<>(ri));
  }

  public static void main(String... args) throws Exception {
    if (args.length != 1) {
      logger.info("USAGE: " + RebarIngesterServer.class.getSimpleName() + " <port-number>");
      System.exit(1);
    }
    
    int port = 0;
    try {
      port = Integer.parseInt(args[0]);
    } catch (NumberFormatException e) {
      logger.error("You passed in a port that was a non-integer: " + args[0]);
      System.exit(1);
    }
    
    try (RebarIngesterServer ris = new RebarIngesterServer(port);) {
      logger.info("Preparing to start RebarIngesterServer on port: " + port);
      ris.start();
    }
  }
}

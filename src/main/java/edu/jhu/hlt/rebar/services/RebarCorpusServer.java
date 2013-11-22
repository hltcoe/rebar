/**
 * 
 */
package edu.jhu.hlt.rebar.services;

import org.apache.accumulo.core.client.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.concrete.CorpusHandler;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.accumulo.RebarCorpusHandler;

/**
 * @author max
 *
 */
public class RebarCorpusServer extends AbstractThriftServer {
  
  private static final Logger logger = LoggerFactory.getLogger(RebarCorpusServer.class);
  
  /**
   * @param port
   * @throws RebarException
   */
  public RebarCorpusServer(int port) throws RebarException {
    this(Constants.getConnector(), port);
  }
  
  public RebarCorpusServer(Connector conn, int port) throws RebarException {
    this(conn, port, new RebarCorpusHandler(conn));
  }

  /**
   * @param port
   * @param processor
   * @throws RebarException
   */
  private RebarCorpusServer(Connector conn, int port, RebarCorpusHandler rch) throws RebarException {
    super(port, rch, new CorpusHandler.Processor<>(rch));
  }
  
  public static void main(String... args) {
    if (args.length != 1) {
      logger.info("USAGE: " + RebarCorpusServer.class.getSimpleName() + " <port-number>");
      System.exit(1);
    }
    
    int port = 0;
    try {
      port = Integer.parseInt(args[0]);
    } catch (NumberFormatException e) {
      logger.error("You passed in a port that was a non-integer: " + args[0]);
      System.exit(1);
    }
    
    try (RebarCorpusServer srv = new RebarCorpusServer(port);) {
      logger.info("Preparing to start RebarCorpusServer on port: " + port);
      srv.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

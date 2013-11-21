/**
 * 
 */
package edu.jhu.hlt.rebar.services;


import org.apache.accumulo.core.client.Connector;
import org.apache.thrift.server.TSimpleServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.concrete.Ingester;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.accumulo.RebarIngester;

/**
 * @author max
 *
 */
public class RebarIngesterServer extends AbstractThriftServer {

  private static final Logger logger = LoggerFactory.getLogger(RebarIngesterServer.class);
  
  private RebarIngester ri;
  private Ingester.Processor<RebarIngester> processor;
  
  /**
   * @throws RebarException 
   * 
   */
  public RebarIngesterServer(int port) throws RebarException {
    this(Constants.getConnector(), port);
  }
  
  /**
   * @throws RebarException 
   * 
   */
  public RebarIngesterServer(Connector conn, int port) throws RebarException {
    super(port);
    this.ri = new RebarIngester(conn);
    this.processor = new Ingester.Processor<RebarIngester>(ri);
    this.args.processor(this.processor);
    this.server = new TSimpleServer(args);
  }

  public static void main(String... args) throws Exception {
    try (RebarIngesterServer ris = new RebarIngesterServer(9990);) {
      ris.start();
    }
  }

  /* (non-Javadoc)
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public void close() throws Exception {
    logger.debug("RebarIngesterServer closing.");
    if (this.server.isServing())
      this.stop();

    this.ri.close();
    this.serverXport.close();
  }

  /* (non-Javadoc)
   * @see java.lang.Runnable#run()
   */
  @Override
  public void run() {
    this.start();
  }
}

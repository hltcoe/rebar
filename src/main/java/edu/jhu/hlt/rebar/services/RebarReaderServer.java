/**
 * 
 */
package edu.jhu.hlt.rebar.services;

import org.apache.accumulo.core.client.Connector;
import org.apache.thrift.server.TSimpleServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.concrete.Reader;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.accumulo.RebarReader;

/**
 * @author max
 *
 */
public class RebarReaderServer extends AbstractThriftServer {
  
  private static final Logger logger = LoggerFactory.getLogger(RebarReaderServer.class);
  
  private RebarReader rr;
  private Reader.Processor<RebarReader> processor;
  
  /**
   * @param port
   * @throws RebarException
   */
  public RebarReaderServer(int port) throws RebarException {
    this(Constants.getConnector(), port);
    // TODO Auto-generated constructor stub
  }
  
  public RebarReaderServer(Connector conn, int port) throws RebarException {
    super(port);
    this.rr = new RebarReader(conn);
    this.processor = new Reader.Processor<RebarReader>(rr);
    this.args.processor(this.processor);
    this.server = new TSimpleServer(this.args);
  }

  /* (non-Javadoc)
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub
    logger.debug("RebarIngesterServer closing.");
    if (this.server.isServing())
      this.stop();

    this.rr.close();
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

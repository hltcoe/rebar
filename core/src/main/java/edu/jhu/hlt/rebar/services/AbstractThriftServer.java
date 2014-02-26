/**
 * 
 */
package edu.jhu.hlt.rebar.services;

import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.accumulo.AbstractAccumuloClient;

/**
 * @author max
 *
 */
public abstract class AbstractThriftServer implements AutoCloseable, Runnable {

  private static final Logger logger = LoggerFactory.getLogger(AbstractThriftServer.class);
  
  protected TServerTransport serverXport;
  protected TServer server;
  protected Args args;
  protected AbstractAccumuloClient aac;
  
  /**
   * 
   */
  protected AbstractThriftServer(int port, AbstractAccumuloClient aac, TProcessor processor) throws RebarException {
    try {
      this.aac = aac;
      this.serverXport = new TServerSocket(port);
      this.args = new Args(this.serverXport);
      this.args.processor(processor);
      this.server = new TSimpleServer(this.args);
    } catch (TTransportException e) {
      throw new RebarException(e);
    }
  }
  
  public final void start() {
    this.server.serve();
  }
  public final void stop() {
    this.server.stop();
  }
  
  public final void run() {
    this.start();
  }
  
  public final void close() throws Exception {
    this.aac.close();
    logger.debug("AbstractThriftServer closing.");
    if (this.server.isServing())
      this.stop();
    this.serverXport.close();
  }
}

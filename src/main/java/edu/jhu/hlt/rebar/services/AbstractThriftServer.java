/**
 * 
 */
package edu.jhu.hlt.rebar.services;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public abstract class AbstractThriftServer implements AutoCloseable, Runnable {

  protected TServerTransport serverXport;
  protected TServer server;
  protected Args args;
  
  /**
   * 
   */
  public AbstractThriftServer(int port) throws RebarException {
    try {
      this.serverXport = new TServerSocket(port);
      this.args = new Args(this.serverXport);
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
}

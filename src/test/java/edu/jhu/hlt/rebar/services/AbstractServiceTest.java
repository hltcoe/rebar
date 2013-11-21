/**
 * 
 */
package edu.jhu.hlt.rebar.services;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import edu.jhu.hlt.rebar.accumulo.AbstractAccumuloTest;

/**
 * @author max
 *
 */
public class AbstractServiceTest extends AbstractAccumuloTest {

  protected Thread serviceThread;
  protected TTransport xport;
  protected TProtocol protocol;
  
  /**
   * @throws TTransportException 
   * @throws AccumuloSecurityException 
   * @throws AccumuloException 
   * 
   */
  protected AbstractServiceTest() {
    
  }
  
  protected void initializeServiceFields() throws AccumuloException, AccumuloSecurityException, TTransportException {
    this.xport = new TSocket("localhost", 9990);
    this.xport.open();
    this.protocol = new TBinaryProtocol(this.xport);
  }
  
}

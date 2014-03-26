/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.fail;
import junit.framework.TestFailure;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * @author max
 *
 */
public class ITestRebarIngester {

//  private RebarIngester ing;
//  private TServerTransport trans;
//  private TServer srv;
//  private MiniAccumuloCluster acc;
//  
//  @Rule
//  public TemporaryFolder testFolder = new TemporaryFolder();
//  
//   
//  
//  /**
//   * @throws java.lang.Exception
//   */
//  @Before
//  public void setUp() throws Exception {
//    this.ing = new RebarIngester();
//    this.trans = new TServerSocket(9091);
//    this.srv = new TSimpleServer(new Args(this.trans));
//    this.acc = new MiniAccumuloCluster(testFolder.newFolder("accumulo"), "secret");
//    this.acc.start();
//  }
//
//  /**
//   * @throws java.lang.Exception
//   */
//  @After
//  public void tearDown() throws Exception {
//    this.acc.stop();
//  }
//
//  @Test
//  public void test() {
//    //fail("Not yet implemented"); // TODO
//  }

}

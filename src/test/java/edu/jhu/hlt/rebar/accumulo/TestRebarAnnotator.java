/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Random;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.maxjthomas.dumpster.Document;

/**
 * @author max
 *
 */
public class TestRebarAnnotator {

  private Instance inst;
  private Connector conn;
  private TSerializer serializer;

  private static final Random rand = new Random();
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.inst = new MockInstance();
    this.conn = this.inst.getConnector("max", new PasswordToken(""));
    this.serializer = new TSerializer(new TBinaryProtocol.Factory());
    
    RebarIngester ri = new RebarIngester(this.conn);
    for (int i = 0; i < 10; i++) {
      Document d = TestRebarIngester.generateMockDocument();
      ri.ingest(d);      
    }
    
    ri.close();
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testAnnotateDocument() throws Exception {
    RebarAnnotator ra = new RebarAnnotator(this.conn);
    //ra.addLanguageId(document, stage, lid);
    ra.close();
  }
}

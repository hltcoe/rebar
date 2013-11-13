/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

/**
 * @author max
 *
 */
public class AbstractAccumuloTest {

  protected Instance inst;
  protected Connector conn;
  protected RebarTableOps tableOps;
  protected TSerializer serializer;
  protected TDeserializer deserializer;

  protected static final Random rand = new Random();
  
  /**
   * 
   */
  public AbstractAccumuloTest() {
    // TODO Auto-generated constructor stub
  }
  
  protected void initialize() throws AccumuloException, AccumuloSecurityException {
    this.inst = new MockInstance();
    this.conn = this.inst.getConnector("max", new PasswordToken(""));
    this.tableOps = new RebarTableOps(conn);
    this.serializer = new TSerializer(new TBinaryProtocol.Factory());
    this.deserializer = new TDeserializer(new TBinaryProtocol.Factory());
  }

}

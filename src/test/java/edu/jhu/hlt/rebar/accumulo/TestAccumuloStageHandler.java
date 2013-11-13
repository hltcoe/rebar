/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.maxjthomas.dumpster.Stage;

import edu.jhu.hlt.rebar.config.RebarConfiguration;

/**
 * @author max
 *
 */
public class TestAccumuloStageHandler {

  private Instance inst;
  private Connector conn;
  private RebarTableOps tableOps;
  private TSerializer serializer;
  private TDeserializer deserializer;
  private AccumuloStageHandler ash;
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.inst = new MockInstance();
    this.conn = this.inst.getConnector("max", new PasswordToken(""));
    this.tableOps = new RebarTableOps(conn);
    this.serializer = new TSerializer(new TBinaryProtocol.Factory());
    this.deserializer = new TDeserializer(new TBinaryProtocol.Factory());
    this.ash = new AccumuloStageHandler(this.conn);
  }
  
  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.AccumuloStageHandler#stageExists(java.lang.String)}.
   * @throws TException 
   */
  @Test
  public void testStageExists() throws TException {
    assertFalse("Shouldn't find any stages at the start.", this.ash.stageExists(generateTestStage().name));
  }
  
  public static int getCurrentUnixTime() {
    long millis = new DateTime().getMillis();
    return ((int) (millis/1000));
  }
  
  public static Stage generateTestStage() {
    return new Stage("stage_foo", "Foo stage for testing", getCurrentUnixTime(), new HashSet<String>());
  }

  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.AccumuloStageHandler#createStage(com.maxjthomas.dumpster.Stage)}.
   * @throws TException 
   * @throws TableNotFoundException 
   */
  @Test
  public void testCreateStage() throws TException, TableNotFoundException {
    Stage s = generateTestStage();
    this.ash.createStage(s);
    
    assertTrue("Should find a table with this stage but didn't.", this.tableOps.tableExists(s.name));
    
    Iterator<Entry<Key, Value>> iter = TestRebarIngester.generateIterator(conn, RebarConfiguration.AVAILABLE_CORPUS_TABLE_NAME, new Range());
    assertTrue("Should find results in the corpus table, but didn't.", iter.hasNext());
    
//    iter = TestRebarIngester.generateIterator(conn, s.name, new Range());
//    assertEquals("Should find an equal number of documents and ids in the corpus.", docSet.size(), TestRebarIngester.countIteratorResults(iter));
//  
  }

  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.AccumuloStageHandler#getStages()}.
   */
  @Test
  public void testGetStages() {
    fail("Not yet implemented"); // TODO
  }

}

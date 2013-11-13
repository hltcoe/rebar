/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

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
    this.ash.close();
  }

  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.AccumuloStageHandler#stageExists(java.lang.String)}.
   * @throws TException 
   */
  @Test
  public void testStageExists() throws TException {
    Stage s = generateTestStage();
    String sName = s.name;
    assertFalse("Shouldn't find any stages at the start.", this.ash.stageExists(sName));
    this.ash.createStage(s);
    assertTrue("Should find the test stage.", this.ash.stageExists(sName));
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
    
    Iterator<Entry<Key, Value>> iter = TestRebarIngester.generateIterator(conn, RebarConfiguration.STAGES_TABLE_NAME, new Range());
    assertTrue("Should find results in the stages table, but didn't.", iter.hasNext());
    
    Stage newS = generateTestStage();
    newS.name = "stage_quxbarfoo";
    this.ash.createStage(newS);
    
    iter = TestRebarIngester.generateIterator(conn, RebarConfiguration.STAGES_TABLE_NAME, new Range());
    assertEquals("Should get 2 stages back.", 2, TestRebarIngester.countIteratorResults(iter));
    
    Stage sDeps = generateTestStage();
    String sDepsName = "stage_with_deps";
    sDeps.name = sDepsName;
    Set<String> depsSet = new HashSet<>();
    depsSet.add(newS.name);
    sDeps.dependencies = depsSet;
    this.ash.createStage(sDeps);
    
    iter = TestRebarIngester.generateIterator(conn, RebarConfiguration.STAGES_TABLE_NAME, new Range());
    assertEquals("Should get 3 stages back.", 3, TestRebarIngester.countIteratorResults(iter));
    
    while (iter.hasNext()) {
      Value v = iter.next().getValue();
      Stage compStage = new Stage();
      this.deserializer.deserialize(compStage, v.get());
      if (compStage.name.equals(sDepsName)) {
        assertEquals("Should get the same thing back from the stage with dependencies.", sDeps, compStage);
        assertEquals("Dependencies should be the same in dependency stage.", depsSet, compStage.dependencies);
        break;
      }
    }
  }
  
  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.AccumuloStageHandler#createStage(com.maxjthomas.dumpster.Stage)}.
   * @throws TException 
   * @throws TableNotFoundException 
   */
  @Test(expected=TException.class)
  public void testCreateStageBadDeps() throws TException, TableNotFoundException {
    Stage s = generateTestStage();
    Set<String> badDeps = new HashSet<>();
    badDeps.add("stage_fooqux");
    s.dependencies = badDeps;
    this.ash.createStage(s);
  }
  
  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.AccumuloStageHandler#createStage(com.maxjthomas.dumpster.Stage)}.
   * @throws TException 
   * @throws TableNotFoundException 
   */
  @Test(expected=TException.class)
  public void testCreateStageTwice() throws TException, TableNotFoundException {
    Stage s = generateTestStage();
    this.ash.createStage(s);
    this.ash.createStage(s);
  }

  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.AccumuloStageHandler#getStages()}.
   * @throws TException 
   */
  @Test
  public void testGetStages() throws TException {
    Stage s = generateTestStage();
    this.ash.createStage(s);
    
    Set<Stage> stages = this.ash.getStages();
    assertEquals("Stages should be equal.", s, stages.iterator().next());
  }
  
  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.AccumuloStageHandler#getStages()}.
   * @throws TException 
   */
  @Test
  public void testGetMultiStages() throws TException {
    Set<Stage> ingestedStages = new HashSet<>();
    Stage s = generateTestStage();
    this.ash.createStage(s);
    ingestedStages.add(s);
    Stage newS = generateTestStage();
    newS.name = "stage_quxqux";
    this.ash.createStage(newS);
    ingestedStages.add(newS);
    
    Set<Stage> stages = this.ash.getStages();
    assertEquals("Stages should be equal.", ingestedStages, stages);
  }

}

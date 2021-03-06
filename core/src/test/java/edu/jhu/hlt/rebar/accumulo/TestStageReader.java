/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.stage.Stage;
import edu.jhu.hlt.rebar.stage.StageCreator;
import edu.jhu.hlt.rebar.stage.StageReader;
import edu.jhu.hlt.rebar.stage.StageType;

/**
 * @author max
 *
 */
public class TestStageReader extends AbstractAccumuloTest {
  
  private StageReader sr;
  private StageCreator ing;
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.sr = new StageReader(this.conn);
    this.ing = new StageCreator(this.conn);
  }
  
  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {

  }

  /**
   * Test method for {@link edu.jhu.hlt.rebar.stage.StageCreator#exists(java.lang.String)}.
   * @throws TException 
   * @throws RebarException 
   */
  @Test
  public void testStageExists() throws RebarException {
    Stage s = generateTestStage();
    String sName = s.getName();
    assertFalse("Shouldn't find any stages at the start.", this.sr.exists(sName));
    ing.create(s);
    assertTrue("Should find an ingested stage.", this.sr.exists(s));
//    this.ash.create(s);
//    assertTrue("Should find stage after ingest.", this.ash.exists(sName));
  }
  
  /**
   * Test method for {@link edu.jhu.hlt.rebar.stage.StageCreator#exists(java.lang.String)}.
   * @throws Exception 
   * @throws TException 
   */
  @Test
  public void testPrinting() throws Exception {
    ing.create(generateTestStage());
    ing.create(generateTestStage("stage_foo_test", "Good stage", new HashSet<String>(), StageType.LANG_ID));
    
    new StageReader(this.conn).printStages();
  }
}

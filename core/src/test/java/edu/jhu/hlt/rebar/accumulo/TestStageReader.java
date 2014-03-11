/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.asphalt.Stage;
import edu.jhu.hlt.asphalt.StageType;
import edu.jhu.hlt.asphalt.services.StageHandler;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;
import edu.jhu.hlt.rebar.stage.StageCreator;
import edu.jhu.hlt.rebar.stage.StageReader;

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
    String sName = s.name;
    assertFalse("Shouldn't find any stages at the start.", this.sr.exists(sName));
    ing.create(s);
    assertTrue("Should find an ingested stage.", this.sr.exists(s));
//    this.ash.create(s);
//    assertTrue("Should find stage after ingest.", this.ash.exists(sName));
  }
  
  /**
   * Test method for {@link edu.jhu.hlt.rebar.stage.StageCreator#exists(java.lang.String)}.
   * @throws TException 
   * @throws RebarException 
   */
  @Test
  public void testPrinting() throws RebarException {
    ing.create(generateTestStage());
    ing.create(generateTestStage("foo_test", "Good stage", new HashSet<String>(), StageType.LANG_ID));
    
    this.sr.printStages();
  }
}

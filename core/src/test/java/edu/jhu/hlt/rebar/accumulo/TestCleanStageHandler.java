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
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;

/**
 * @author max
 *
 */
public class TestCleanStageHandler extends AbstractAccumuloTest {

  private CleanStageHandler ash;
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.ash = new CleanStageHandler(this.conn);
  }
  
  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    this.ash.close();
  }

  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.CleanStageHandler#exists(java.lang.String)}.
   * @throws TException 
   * @throws RebarException 
   */
  @Test
  public void testStageExists() throws RebarException {
    Stage s = generateTestStage();
    String sName = s.name;
    assertFalse("Shouldn't find any stages at the start.", this.ash.exists(sName));
    
    this.ash.create(s);
    assertTrue("Should find stage after ingest.", this.ash.exists(sName));
  }
}
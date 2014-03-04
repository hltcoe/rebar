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
public class TestRebarStageHandler extends AbstractAccumuloTest {

  private RebarStageHandler ash;
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.ash = new RebarStageHandler(this.conn);
  }
  
  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    this.ash.close();
  }

  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.RebarStageHandler#stageExists(java.lang.String)}.
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

  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.RebarStageHandler#createStage(com.maxjthomas.dumpster.Stage)}.
   * @throws TException 
   * @throws TableNotFoundException 
   */
  @Test
  public void testCreateStage() throws TException, TableNotFoundException {
    Stage s = generateTestStage();
    this.ash.createStage(s);
    
    Iterator<Entry<Key, Value>> iter = TestRebarIngester.generateIterator(conn, Constants.STAGES_TABLE_NAME, new Range());
    assertTrue("Should find results in the stages table, but didn't.", iter.hasNext());
    
    Stage newS = generateTestStage();
    newS.name = "stage_quxbarfoo";
    this.ash.createStage(newS);
    
    iter = TestRebarIngester.generateIterator(conn, Constants.STAGES_TABLE_NAME, new Range());
    assertEquals("Should get 2 stages back.", 2, Util.countIteratorResults(iter));
    
    Stage sDeps = generateTestStage();
    String sDepsName = "stage_with_deps";
    sDeps.name = sDepsName;
    Set<String> depsSet = new HashSet<>();
    depsSet.add(newS.name);
    sDeps.dependencies = depsSet;
    this.ash.createStage(sDeps);
    
    iter = TestRebarIngester.generateIterator(conn, Constants.STAGES_TABLE_NAME, new Range());
    assertEquals("Should get 3 stages back.", 3, Util.countIteratorResults(iter));
    
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
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.RebarStageHandler#createStage(com.maxjthomas.dumpster.Stage)}.
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
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.RebarStageHandler#createStage(com.maxjthomas.dumpster.Stage)}.
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
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.RebarStageHandler#getStages()}.
   * @throws Exception 
   * @throws RebarException 
   */
  @Test
  public void testGetStages() throws RebarException, Exception {
    Stage s = generateTestStage();
    this.ash.createStage(s);
    
    List<Stage> stages = this.ash.getStages();
    assertEquals("Stages should be equal.", s, stages.iterator().next());
    
//    try (RebarStageHandler handler = new RebarStageHandler(conn);) {
//      stages = handler.getStages();
//      assertEquals("Stages should be equal despite different handler.", s, stages.iterator().next());
//    }
  }
  
  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.RebarStageHandler#getStages()}.
   * @throws TException 
   */
  @Test
  public void testGetMultiStages() throws TException {
    List<Stage> ingestedStages = new ArrayList<>();
    Stage s = generateTestStage();
    this.ash.createStage(s);
    ingestedStages.add(s);
    Stage newS = generateTestStage();
    newS.name = "stage_quxqux";
    this.ash.createStage(newS);
    ingestedStages.add(newS);
    
    List<Stage> stages = this.ash.getStages();
    assertTrue("Stages should be equal.", ingestedStages.containsAll(stages));
  }
  
//  @Test
//  public void testAddAnnotatedDocument() throws RebarException, Exception {
//    Stage s = generateTestStage();
//    
//    Set<Communication> docSet = TestRebarIngester.generateMockDocumentSet(10);
//    for (Communication d : docSet) {
//      this.ash.addAnnotatedDocument(s, d);
//    }
//    
//    assertEquals("Should find 10 document IDs in the annotated-docs column:", 10, this.ash.getAnnotatedDocumentCount(s));
//  }
  
//  @Test
//  public void testGetAnnotatedDocumentCount() throws RebarException, Exception {
//    Stage s = generateTestStage();
//    
//    Set<Communication> docSet = TestRebarIngester.generateMockDocumentSet(10);
//    try (RebarAnnotator ra = new RebarAnnotator(this.conn);) {
//      for (Communication d : docSet) {
//        ra.addLanguageId(d, s, TestRebarAnnotator.generateLanguageIdentification(d));
//      }
//    }
//    
//    assertEquals("Should find 10 document IDs in the annotated-docs column:", 10, this.ash.getAnnotatedDocumentCount(s));
//  }
  
  @Test
  public void testGetAnnotatedDocumentIds() throws RebarException, Exception {
    Stage s = generateTestStage();
    
    Set<Communication> docSet = TestRebarIngester.generateMockDocumentSet(10);
    for (Communication d : docSet) {
      this.ash.addAnnotatedDocument(s, d);
    }
    
    Set<String> idSet = new HashSet<>();
    for (Communication d : docSet)
      idSet.add(d.id);
    
    assertEquals("Should have equal ID sets:", idSet, this.ash.getAnnotatedDocumentIds(s));
  }
}

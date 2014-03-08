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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.asphalt.AsphaltException;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.Util;

/**
 * @author max
 *
 */
public class TestRebarCorpusHandler extends AbstractAccumuloTest {

  private static final Logger logger = LoggerFactory.getLogger(TestRebarCorpusHandler.class);
  
  private RebarCorpusHandler rch;
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.rch = new RebarCorpusHandler(this.conn);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    //this.tableOps.deleteTable(Constants.AVAILABLE_CORPUS_TABLE_NAME);
  }
  
  @Test(expected=AsphaltException.class)
  public void testCreateBadNameCorpus() throws Exception {
    String testCorpus = "foo";
    assertTrue(this.tableOps.tableExists(Constants.AVAILABLE_CORPUS_TABLE_NAME));
    rch.createCorpus(testCorpus, AbstractAccumuloTest.generateMockDocumentList(10));
    rch.close();
  }
  
  @Test(expected=AsphaltException.class)
  public void testCreateBadDocsCorpus() throws Exception {
    String testCorpus = "corpus_foo";
    assertTrue(this.tableOps.tableExists(Constants.AVAILABLE_CORPUS_TABLE_NAME));
    rch.createCorpus(testCorpus, new ArrayList<Communication>());
    rch.close();
  }
  
  @Test(expected=AsphaltException.class)
  public void testCreateDupeCorpus() throws Exception {
    String testCorpus = "corpus_foo";
    assertTrue(this.tableOps.tableExists(Constants.AVAILABLE_CORPUS_TABLE_NAME));
    rch.createCorpus(testCorpus, AbstractAccumuloTest.generateMockDocumentList(10));
    rch.createCorpus(testCorpus, AbstractAccumuloTest.generateMockDocumentList(10));
    rch.close();
  }

  @Test
  public void testCreateCorpus() throws Exception {
    String testCorpus = "corpus_foo";
    assertTrue(this.tableOps.tableExists(Constants.AVAILABLE_CORPUS_TABLE_NAME));
    List<Communication> docList = AbstractAccumuloTest.generateMockDocumentList(10);
    rch.createCorpus(testCorpus, docList);
    rch.close();
    
    //Scanner sc = this.conn.createScanner("available_corpora", Constants.NO_AUTHS);
    Iterator<Entry<Key, Value>> iter = TestRebarIngester.generateIterator(conn, Constants.AVAILABLE_CORPUS_TABLE_NAME, new Range());
    assertTrue("Should find results in the corpus table, but didn't.", iter.hasNext());
    
    iter = TestRebarIngester.generateIterator(conn, testCorpus, new Range());
    assertEquals("Should find an equal number of documents and ids in the corpus.", docList.size(), Util.countIteratorResults(iter));
  }
  
  @Test
  public void testDeleteCorpus() throws Exception {
    String testCorpus = "corpus_foo";
    assertTrue(this.tableOps.tableExists(Constants.AVAILABLE_CORPUS_TABLE_NAME));
    List<Communication> docList = AbstractAccumuloTest.generateMockDocumentList(10);
    rch.createCorpus(testCorpus, docList);
    rch.deleteCorpus(testCorpus);
    rch.close();

    assertFalse("Should NOT find this corpus table!", this.tableOps.tableExists(testCorpus));
    
    //Scanner sc = this.conn.createScanner("available_corpora", Constants.NO_AUTHS);
    Iterator<Entry<Key, Value>> iter = TestRebarIngester.generateIterator(conn, Constants.AVAILABLE_CORPUS_TABLE_NAME, new Range());
    boolean hasResults = iter.hasNext();
    while (iter.hasNext()) {
      Entry<Key, Value> e = iter.next();
      Key k = e.getKey();
      logger.error("FAILURE: Got key: " + k.toString());
    }
    
    assertFalse("Should NOT find results in the corpus table, but did.", hasResults);
  }
  
  @Test(expected=AsphaltException.class)
  public void testDeleteNonExistentCorpus() throws Exception {
    String testCorpus = "corpus_foo";
    rch.deleteCorpus(testCorpus);
  }
  
  @Test
  public void testCorpusExists() throws Exception {
    String testCorpus = "corpus_foo";
    assertTrue(this.tableOps.tableExists(Constants.AVAILABLE_CORPUS_TABLE_NAME));
    List<Communication> docList = AbstractAccumuloTest.generateMockDocumentList(10);
    rch.createCorpus(testCorpus, docList);
    rch.close();

    assertTrue("Did not find the corpus table.", this.tableOps.tableExists(testCorpus));
    //Scanner sc = this.conn.createScanner("available_corpora", Constants.NO_AUTHS);
    Iterator<Entry<Key, Value>> iter = TestRebarIngester.generateIterator(conn, Constants.AVAILABLE_CORPUS_TABLE_NAME, new Range());
    assertTrue("Should find results in the corpus table, but did.", iter.hasNext());
  }
  
  @Test
  public void testListCorpora() throws Exception {
    String testCorpus = "corpus_foo";
    List<Communication> docList = AbstractAccumuloTest.generateMockDocumentList(10);
    assertEquals("Shouldn't find any corpora initially.", 0, rch.listCorpora().size());
    
    rch.createCorpus(testCorpus, docList);
    assertEquals("Should find 1 corpus", 1, rch.listCorpora().size());
    
    rch.createCorpus("corpus_foo_bar", AbstractAccumuloTest.generateMockDocumentList(10));
    rch.createCorpus("corpus_qux_bar", AbstractAccumuloTest.generateMockDocumentList(10));
    assertEquals("Should find 3 corpora", 3, rch.listCorpora().size());
    
    rch.deleteCorpus(testCorpus);
    assertEquals("Should find 2 corpora after delete", 2, rch.listCorpora().size());
    rch.close();
  }
//  
//  @Test
//  public void testGetCorpusDocSet() throws Exception {
//    String testCorpus = "corpus_foo";
//    List<Communication> docSet = this.ingestDocuments(25);
//    
//    rch.createCorpus(testCorpus, new ArrayList<>(new HashSet<>(docSet)));
//    List<Communication> retDocSet = rch.getCorpusCommunicationSet(testCorpus);
//    rch.close();
//    
//    assertTrue("Should get the same docs in and out, but didn't.", retDocSet.containsAll(docSet));
//  }
}

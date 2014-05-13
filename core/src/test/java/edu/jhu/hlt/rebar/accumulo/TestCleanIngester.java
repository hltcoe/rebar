/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.RebarException;


/**
 * @author max
 *
 */
public class TestCleanIngester extends AbstractAccumuloTest {

  private static final Logger logger = LoggerFactory.getLogger(TestCleanIngester.class);
  
  CleanIngester ci;
  CommunicationReader cr;
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.ci = new CleanIngester(this.conn);
    this.cr = new CommunicationReader(this.conn);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.CleanIngester#isDocumentIngested(edu.jhu.hlt.concrete.Communication)}.
   * 
   * @throws RebarException 
   * @throws TableNotFoundException 
   * @throws MutationsRejectedException 
   */
  @Test
  public void testIsDocumentIngested() throws RebarException, TableNotFoundException, MutationsRejectedException {
    assertFalse(cr.exists("bar"));
    Communication c = generateMockDocument();
    Communication c2 = generateMockDocument();
    c.startTime = 39595830;
    c2.startTime = 395958301;
    assertFalse(cr.exists(c));
    ci.ingest(c);
    assertTrue(cr.exists(c));
    assertFalse(cr.exists("bar"));
    ci.ingest(c);
    ci.ingest(c2);
    assertTrue(cr.exists(c));
    assertTrue(cr.exists(c2));
  }
  
  /*
   * TODO: reimplement below junk into CleanIngester tests.
   */
//  @Test
//  public void testInsertDocument() throws TException, RebarException, TableNotFoundException {
//    Communication d = generateMockDocument();
//    //String rowId = RebarIngester.generateRowId(d);
//    String docId = d.id;
//    byte[] dbytes = this.serializer.serialize(d);
//    
//    RebarIngester rebar = new RebarIngester(this.conn);
//    rebar.ingest(d);
//    rebar.close();
//    
//    Iterator<Entry<Key, Value>> iter = generateIterator(this.conn, edu.jhu.hlt.rebar.Constants.DOCUMENT_TABLE_NAME, new Range(docId));
//    assertTrue("Should find results in accumulo.", iter.hasNext());
//    assertEquals(0, iter.next().getValue().compareTo(dbytes));
//  }
//  
//  @Test
//  public void testInsertManyDocuments() throws TException, RebarException, TableNotFoundException {
//    int nDocs = 50;
//    Set<Communication> docs = generateMockDocumentSet(nDocs);
//    
//    RebarIngester rebar = new RebarIngester(this.conn);
//    for (Communication d : docs)
//      rebar.ingest(d);
//    rebar.close();
//    
//    Iterator<Entry<Key, Value>> iter = generateIterator(this.conn, edu.jhu.hlt.rebar.Constants.DOCUMENT_TABLE_NAME, new Range());
//    assertEquals("Should find a few results in accumulo.", nDocs, Util.countIteratorResults(iter));
//    
//    iter = generateIterator(this.conn, edu.jhu.hlt.rebar.Constants.DOCUMENT_TABLE_NAME, new Range());
//    Set<Communication> fetchDocs = new HashSet<>(nDocs);
//    while(iter.hasNext()) {
//      Communication d = new Communication();
//      Value v = iter.next().getValue();
//      this.deserializer.deserialize(d, v.get());
//      fetchDocs.add(d);
//    }
//    
//    BatchScanner bsc = this.conn.createBatchScanner(edu.jhu.hlt.rebar.Constants.DOCUMENT_TABLE_NAME, Configuration.getAuths(), 10);
//    List<Range> rangeList = new ArrayList<>();
//    rangeList.add(new Range(docs.iterator().next().id));
//    bsc.setRanges(rangeList);
//    iter = bsc.iterator();
//    while (iter.hasNext()) {
//      Entry<Key, Value> e = iter.next();
//      Key k = e.getKey();
//      Value v = e.getValue();
//    }
//    
//    assertEquals("Input and output sets should be the same, but weren't.", docs, fetchDocs);
//  }

}

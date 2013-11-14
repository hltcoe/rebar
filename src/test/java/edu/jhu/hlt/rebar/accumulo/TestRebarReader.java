/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.maxjthomas.dumpster.Document;
import com.maxjthomas.dumpster.LangId;
import com.maxjthomas.dumpster.Stage;

import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;
import edu.jhu.hlt.rebar.config.RebarConfiguration;

/**
 * @author max
 * 
 */
public class TestRebarReader extends AbstractAccumuloTest {

  private RebarReader rr;
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.rr = new RebarReader(this.conn);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    this.rr.close();
  }

  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.RebarReader#getAnnotatedDocuments(com.maxjthomas.dumpster.Stage)}.
   * 
   * @throws Exception
   * @throws RebarException
   */
  @Test
  public void testGetAnnotatedDocuments() throws RebarException, Exception {
    int nDocs = 3;
    List<Document> docList = new ArrayList<>(generateMockDocumentSet(nDocs));
    try (RebarIngester re = new RebarIngester(this.conn);) {
      for (Document d : docList)
        re.ingest(d);
      
    }
    
    Set<Document> docsWithLid = new HashSet<>();

    List<LangId> langIdList = new ArrayList<>();
    Stage s = generateTestStage();
    try (RebarAnnotator ra = new RebarAnnotator(this.conn);) {
      for (Document d : docList) {
        LangId mockLid = generateLangId(d);
        ra.addLanguageId(d, s, mockLid);
        langIdList.add(mockLid);
        Document newDoc = new Document(d);
        newDoc.lid = mockLid;
        docsWithLid.add(newDoc);
      }
    }

    int annotatedDocs = 0;
    Set<String> idSet;
    try (AccumuloStageHandler ash = new AccumuloStageHandler(this.conn);) {
      annotatedDocs = ash.getAnnotatedDocumentCount(s);
      idSet = ash.getAnnotatedDocumentIds(s);
    }

    assertEquals("Should get n annotated docs: (n = " + nDocs + ")", nDocs, annotatedDocs);
    BatchScanner bsc = this.rr.createScanner(s, idSet);
    assertEquals("Should get " + nDocs + " entries in this batch scanner.", 3, Util.countIteratorResults(bsc.iterator()));
    bsc.close();

    Set<Document> fetchedDocs = this.rr.getAnnotatedDocuments(s);
    assertEquals("Documents with LID should be the same.", docsWithLid, fetchedDocs);
  }

}

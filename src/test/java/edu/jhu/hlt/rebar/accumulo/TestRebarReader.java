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

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.initialize();
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  private BatchScanner createScanner(Stage stageOfInterest, Set<String> idSet) throws RebarException {
    List<Range> rangeList = new ArrayList<>();
    for (String id : idSet)
      rangeList.add(new Range(id));
    
    try {
      String sName = stageOfInterest.name;
      BatchScanner bsc = this.conn.createBatchScanner(Constants.DOCUMENT_TABLE_NAME, RebarConfiguration.getAuths(), 8);
      bsc.setRanges(rangeList);
      bsc.fetchColumnFamily(new Text(Constants.DOCUMENT_COLF));
      bsc.fetchColumn(new Text(Constants.DOCUMENT_ANNOTATION_COLF), new Text(sName));
      bsc.addScanIterator(new IteratorSetting(1000, "wholeRows", WholeRowIterator.class));
      return bsc;
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }
  
  private Document getRoot(Map<Key, Value> decodedRow) throws TException {
    Document d = new Document();
    for (Map.Entry<Key, Value> entry : decodedRow.entrySet()) 
      if (entry.getKey().compareColumnFamily(new Text(Constants.DOCUMENT_COLF)) == 0) 
        this.deserializer.deserialize(d, entry.getValue().get());
      
    return d;
  }

  private Set<Document> constructDocumentSet(Stage s, Set<String> docIds) throws RebarException, TException, IOException {
    Set<Document> docSet = new HashSet<>();

    BatchScanner bsc = this.createScanner(s, docIds);
    Iterator<Entry<Key, Value>> iter = bsc.iterator();
    while (iter.hasNext()) {
      Entry<Key, Value> e = iter.next();
      Map<Key, Value> rows = WholeRowIterator.decodeRow(e.getKey(), e.getValue());
      Document root = this.getRoot(rows);
      for (Entry<Key, Value> r : rows.entrySet()) {
        if (r.getKey().compareColumnQualifier(new Text(s.name)) == 0) {
          // this is the stage we want.
          switch (s.type) {
          case LANG_ID:
            LangId lid = new LangId();
            this.deserializer.deserialize(lid, r.getValue().get());
            root.setLid(lid);
            break;
          default:
            throw new IllegalArgumentException("Case: " + s.type.toString() + " not handled yet.");
          }
        }
      }
      
      docSet.add(root);
    }

    return docSet;
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
    BatchScanner bsc = this.createScanner(s, idSet);
    assertEquals("Should get " + nDocs + " entries in this batch scanner.", 3, Util.countIteratorResults(bsc.iterator()));
    bsc.close();

    Set<Document> fetchedDocs = this.constructDocumentSet(s, idSet);
    assertEquals("Documents with LID should be the same.", docsWithLid, fetchedDocs);
  }

}

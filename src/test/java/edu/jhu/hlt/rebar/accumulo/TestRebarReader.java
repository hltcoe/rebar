/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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

  public Set<Document> constructDocumentSet(Iterator<Entry<Key, Value>> iter) throws TException {
    Set<Document> docSet = new HashSet<>();
    Map<String, Entry<Key, Value>> rowIdToEntryMap = new HashMap<>();
    while (iter.hasNext()) {
      Entry<Key, Value> e = iter.next();
      Key k = e.getKey();
      String rowId = k.getRow().toString();
      if (rowIdToEntryMap.containsKey(rowId)) {
        // we have the other stage, do some work.
        String colF = k.getColumnFamily().toString();
        if (colF.equals(Constants.DOCUMENT_COLF)) {
          // current entry is the document
          Document d = new Document();
          this.deserializer.deserialize(d, e.getValue().get());

          // we now have the document; merge in the annotation.
          Entry<Key, Value> mapped = rowIdToEntryMap.get(rowId);

        }
      } else {
        rowIdToEntryMap.put(rowId, e);
      }
    }

    return docSet;
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

  public Set<Document> slowConstructDocumentSet(Stage s, Set<String> docIds) throws RebarException {
    Set<Document> docSet = new HashSet<>();
    String stageName = s.name;

    BatchScanner bsc = this.createScanner(s, docIds);
    Iterator<Entry<Key, Value>> iter = bsc.iterator();
    while (iter.hasNext()) {
      Entry<Key, Value> e = iter.next();
      Map<Key, Value> rows = WholeRowIterator.decodeRow(e.getKey(), e.getValue());
      Document root = getRoot(rows);
      for (Entry<Key, Value> r : rows.entrySet()) {
        if (r.getKey().compareColumnQualifier(new Text(s.name)) == 0) {
          // this is the stage we want.
          switch (s.type) {
          case LANG_ID:
            LangId lid = new LangId();
            this.deserializer.deserialize(lid, e.getValue());
            root.setLid(lid);
          
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
    List<Document> docList = new ArrayList<>(generateMockDocumentSet(10));
    try (RebarIngester re = new RebarIngester(this.conn);) {
      for (Document d : docList) {
        re.ingest(d);
      }
    }

    List<LangId> langIdList = new ArrayList<>();
    Stage s = generateTestStage();
    try (RebarAnnotator ra = new RebarAnnotator(this.conn);) {
      for (Document d : docList) {
        LangId mockLid = generateLangId(d);
        ra.addLanguageId(d, s, mockLid);
        langIdList.add(mockLid);
      }
    }

    int annotatedDocs = 0;
    Set<String> idSet;
    try (AccumuloStageHandler ash = new AccumuloStageHandler(this.conn);) {
      annotatedDocs = ash.getAnnotatedDocumentCount(s);
      idSet = ash.getAnnotatedDocumentIds(s);
    }

    assertEquals("Should get 10 annotated docs:", 10, annotatedDocs);

    Scanner sc = this.conn.createScanner(Constants.DOCUMENT_TABLE_NAME, RebarConfiguration.getAuths());
    sc.setRange(new Range());
    sc.fetchColumnFamily(new Text(Constants.DOCUMENT_COLF));
    sc.fetchColumn(new Text(Constants.DOCUMENT_ANNOTATION_COLF), new Text(s.name));
    Iterator<Entry<Key, Value>> iter = sc.iterator();

    int counter = 0;
    while (iter.hasNext()) {
      Entry<Key, Value> e = iter.next();
      counter++;
      Key k = e.getKey();
      String colF = k.getColumnFamily().toString();

      Document newDoc = new Document();

      if (colF.equals(Constants.DOCUMENT_COLF)) {
        this.deserializer.deserialize(newDoc, e.getValue().get());
      } else if (colF.equals(Constants.DOCUMENT_ANNOTATION_COLF)) {

      } else {
        fail("Got bad ColF: " + colF);
      }
    }

    assertEquals("Should get 20 rows.", 20, counter);
  }

}

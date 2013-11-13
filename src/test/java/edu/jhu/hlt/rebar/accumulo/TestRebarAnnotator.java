/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.maxjthomas.dumpster.Document;
import com.maxjthomas.dumpster.LangId;
import com.maxjthomas.dumpster.Stage;

import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.Util;
import edu.jhu.hlt.rebar.config.RebarConfiguration;

/**
 * @author max
 * 
 */
public class TestRebarAnnotator extends AbstractAccumuloTest {

  private Set<Document> docSet;
  private RebarAnnotator ra;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.ra = new RebarAnnotator(conn);

    docSet = new HashSet<>();
    RebarIngester ri = new RebarIngester(this.conn);
    for (int i = 0; i < 10; i++) {
      Document d = TestRebarIngester.generateMockDocument();
      ri.ingest(d);
      docSet.add(d);
    }

    ri.close();
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }
  
  @Test
  public void testAnnotateDocumentNoStage() throws Exception {
//    Stage newStage = new Stage("stage_max_lid_test", "Testing stage for LID", Util.getCurrentUnixTime(), new HashSet<String>());
    Stage newStage = TestAccumuloStageHandler.generateTestStage();

    List<LangId> lidList = new ArrayList<>();
    for (Document d : this.docSet) {
      LangId lid = generateLangId(d);
      lidList.add(lid);
      this.ra.addLanguageId(d, newStage, lid);
    }

    Iterator<Entry<Key, Value>> iter = TestRebarIngester.generateIterator(conn, Constants.DOCUMENT_TABLE_NAME, new Range());
    assertEquals("Should get 20 total rows.", 20, Util.countIteratorResults(iter));
    try (AccumuloStageHandler ashy = new AccumuloStageHandler(this.conn);) {
      Set<Stage> stageSet = ashy.getStages();
      assertTrue("Should get a stage added.", stageSet.size() > 0);
    }
    
    this.ra.close();
  }

  @Test
  public void testAnnotateDocument() throws Exception {
//    Stage newStage = new Stage("stage_max_lid_test", "Testing stage for LID", Util.getCurrentUnixTime(), new HashSet<String>());
    Stage newStage = TestAccumuloStageHandler.generateTestStage();
    
    try (AccumuloStageHandler ash = new AccumuloStageHandler(conn);) {
      ash.createStage(newStage);
    }

    List<LangId> lidList = new ArrayList<>();
    for (Document d : this.docSet) {
      LangId lid = generateLangId(d);
      lidList.add(lid);
      this.ra.addLanguageId(d, newStage, lid);
    }

    Iterator<Entry<Key, Value>> iter = TestRebarIngester.generateIterator(conn, Constants.DOCUMENT_TABLE_NAME, new Range());
    assertEquals("Should get 20 total rows.", 20, Util.countIteratorResults(iter));
    
    Scanner sc = this.conn.createScanner(Constants.DOCUMENT_TABLE_NAME, RebarConfiguration.getAuths());
    sc.setRange(new Range());
    sc.fetchColumn(new Text(Constants.DOCUMENT_ANNOTATION_COLF), new Text(newStage.name));
    iter = sc.iterator();
    assertEquals("Should get 10 annotations: ", 10, Util.countIteratorResults(iter));

    List<Document> docList = new ArrayList<>(docSet);
    for (int i = 0; i < 10; i++) {
      Document d = docList.get(i);
      LangId lid = lidList.get(i);
      String id = d.id;
      Range r = new Range(id);
      iter = TestRebarIngester.generateIterator(conn, Constants.DOCUMENT_TABLE_NAME, r);
      while (iter.hasNext()) {
        Entry<Key, Value> e = iter.next();
        Key k = e.getKey();
        String colF = k.getColumnFamily().toString();
        if (colF.equals(Constants.DOCUMENT_COLF)) {
          Document dser = new Document();
          this.deserializer.deserialize(dser, e.getValue().get());
          assertEquals("Should get a document from document colf.", d, dser);
        } else if (colF.equals(Constants.DOCUMENT_ANNOTATION_COLF)) {
          LangId dser = new LangId();
          this.deserializer.deserialize(dser, e.getValue().get());
          assertEquals("Should get a LID from annotation colf.", lid, dser);
        } else {
          fail("Column family was bad: " + colF);
        }
      }
    }
    
    this.ra.close();
  }
}

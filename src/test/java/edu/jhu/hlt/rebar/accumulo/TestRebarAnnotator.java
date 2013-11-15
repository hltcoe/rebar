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
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.maxjthomas.dumpster.Document;
import com.maxjthomas.dumpster.LangId;
import com.maxjthomas.dumpster.Stage;
import com.maxjthomas.dumpster.Type;

import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Util;

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
    this.ra.close();
  }
  
  @Test
  public void testAnnotateDocumentNoStage() throws Exception {
//    Stage newStage = new Stage("stage_max_lid_test", "Testing stage for LID", Util.getCurrentUnixTime(), new HashSet<String>());
    Stage newStage = TestRebarStageHandler.generateTestStage();

    List<LangId> lidList = new ArrayList<>();
    for (Document d : this.docSet) {
      LangId lid = generateLangId(d);
      lidList.add(lid);
      this.ra.addLanguageId(d, newStage, lid);
    }

    Iterator<Entry<Key, Value>> iter = TestRebarIngester.generateIterator(conn, Constants.DOCUMENT_TABLE_NAME, new Range());
    assertEquals("Should get 20 total rows.", 20, Util.countIteratorResults(iter));
    try (RebarStageHandler ashy = new RebarStageHandler(this.conn);) {
      Set<Stage> stageSet = ashy.getStages();
      assertTrue("Should get a stage added.", stageSet.size() > 0);
    }
  }
  
  @Test(expected=TException.class)
  public void testAnnotateDocumentTwice() throws Exception {
//    
    Stage newStage = TestRebarStageHandler.generateTestStage();

    List<LangId> lidList = new ArrayList<>();
    for (Document d : this.docSet) {
      LangId lid = generateLangId(d);
      lidList.add(lid);
      this.ra.addLanguageId(d, newStage, lid);
      this.ra.addLanguageId(d, newStage, lid);
    }
  }
  
  @Test(expected=TException.class)
  public void testAnnotateWrongType() throws Exception {
//    
    Stage newStage = TestRebarStageHandler.generateTestStage();
    newStage.type = Type.LANG_PRED;

    List<LangId> lidList = new ArrayList<>();
    for (Document d : this.docSet) {
      LangId lid = generateLangId(d);
      lidList.add(lid);
      this.ra.addLanguageId(d, newStage, lid);
    }
  }

  @Test
  public void testAnnotateDocument() throws Exception {
//    Stage newStage = new Stage("stage_max_lid_test", "Testing stage for LID", Util.getCurrentUnixTime(), new HashSet<String>());
    Stage newStage = TestRebarStageHandler.generateTestStage();
    
    try (RebarStageHandler ash = new RebarStageHandler(conn);) {
      ash.createStage(newStage);
    }

    List<LangId> lidList = new ArrayList<>();
    for (Document d : this.docSet) {
      LangId lid = generateLangId(d);
      lidList.add(lid);
      this.ra.addLanguageId(d, newStage, lid);
    }
    
    Stage stageTwo = generateTestStage();
    stageTwo.name = "stage_two_test";
    List<LangId> lidListTwo = new ArrayList<>();
    for (Document d : this.docSet) {
      LangId lid = generateLangId(d);
      lidListTwo.add(lid);
      this.ra.addLanguageId(d, stageTwo, lid);
    }

    Iterator<Entry<Key, Value>> iter = TestRebarIngester.generateIterator(conn, Constants.DOCUMENT_TABLE_NAME, new Range());
    assertEquals("Should get 30 total rows.", 30, Util.countIteratorResults(iter));
    
    Scanner sc = this.conn.createScanner(Constants.DOCUMENT_TABLE_NAME, Configuration.getAuths());
    sc.setRange(new Range());
    sc.fetchColumn(new Text(Constants.DOCUMENT_ANNOTATION_COLF), new Text(newStage.name));
    iter = sc.iterator();
    assertEquals("Should get 10 annotations: ", 10, Util.countIteratorResults(iter));

    List<Document> docList = new ArrayList<>(docSet);
    for (int i = 0; i < 10; i++) {
      Document d = docList.get(i);
      LangId lid = lidList.get(i);
      LangId lidTwo = lidListTwo.get(i);
      String id = d.id;
      Range r = new Range(id);
      iter = TestRebarIngester.generateIterator(conn, Constants.DOCUMENT_TABLE_NAME, r);
      while (iter.hasNext()) {
        Entry<Key, Value> e = iter.next();
        Key k = e.getKey();
        if (k.compareColumnFamily(new Text(Constants.DOCUMENT_COLF)) == 0) {
          Document dser = new Document();
          this.deserializer.deserialize(dser, e.getValue().get());
          assertEquals("Should get a document from document colf.", d, dser);
        } else if (k.compareColumnQualifier(new Text(newStage.name)) == 0) {
          LangId dser = new LangId();
          this.deserializer.deserialize(dser, e.getValue().get());
          assertEquals("Should get a LID from annotation colf.", lid, dser);
        } else if (k.compareColumnQualifier(new Text(stageTwo.name)) == 0) {
          LangId dser = new LangId();
          this.deserializer.deserialize(dser, e.getValue().get());
          assertEquals("Should get a LID from annotation colf.", lidTwo, dser);
        } else {
          fail("Column family was bad: " + k.getColumnFamily().toString());
        }
      }
    }
  }
}

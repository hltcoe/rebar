/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.maxjthomas.dumpster.Document;
import com.maxjthomas.dumpster.LangId;
import com.maxjthomas.dumpster.Stage;

import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.util.RebarUtil;

/**
 * @author max
 * 
 */
public class TestRebarAnnotator {

  private Set<Document> docSet;
  private Instance inst;
  private Connector conn;
  private TSerializer serializer;
  private TDeserializer deserializer;
  private RebarAnnotator ra;

  private static final Random rand = new Random();

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.inst = new MockInstance();
    this.conn = this.inst.getConnector("max", new PasswordToken(""));
    this.serializer = new TSerializer(new TBinaryProtocol.Factory());
    this.deserializer = new TDeserializer(new TBinaryProtocol.Factory());
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

  static Map<String, Double> generateLidMap() {
    Map<String, Double> langIdMap = new HashMap<>();
    langIdMap.put("eng", rand.nextDouble());
    langIdMap.put("spa", rand.nextDouble());
    langIdMap.put("fra", rand.nextDouble());
    return langIdMap;
  }

  static LangId generateLangId(Document d) {
    LangId lid = new LangId();
    lid.id = d.id + "_LID";
    lid.name = "max_lid_test";
    lid.version = "v1";
    lid.languageToProbabilityMap = generateLidMap();
    return lid;
  }
  
  static Set<LangId> generateLangIdSet(Set<Document> docSet) {
    Set<LangId> lidSet = new HashSet<>();
    for (Document d : docSet) 
      lidSet.add(generateLangId(d));
    
    return lidSet;
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }
  
  @Test
  public void testAnnotateDocumentNoStage() throws Exception {
    Stage newStage = new Stage("stage_max_lid_test", "Testing stage for LID", TestAccumuloStageHandler.getCurrentUnixTime(), new HashSet<String>());
//    Stage newStage = TestAccumuloStageHandler.generateTestStage();

    List<LangId> lidList = new ArrayList<>();
    for (Document d : this.docSet) {
      LangId lid = generateLangId(d);
      lidList.add(lid);
      this.ra.addLanguageId(d, newStage, lid);
    }

    Iterator<Entry<Key, Value>> iter = TestRebarIngester.generateIterator(conn, Constants.DOCUMENT_TABLE_NAME, new Range());
    assertEquals("Should get 20 total rows.", 20, RebarUtil.countIteratorResults(iter));
    try (AccumuloStageHandler ashy = new AccumuloStageHandler(this.conn);) {
      Set<Stage> stageSet = ashy.getStages();
      assertTrue("Should get a stage added.", stageSet.size() > 0);
    }
    
    this.ra.close();
  }

  @Test
  public void testAnnotateDocument() throws Exception {
    Stage newStage = new Stage("stage_max_lid_test", "Testing stage for LID", TestAccumuloStageHandler.getCurrentUnixTime(), new HashSet<String>());

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
    assertEquals("Should get 20 total rows.", 20, RebarUtil.countIteratorResults(iter));

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

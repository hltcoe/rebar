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
import com.maxjthomas.dumpster.LanguagePrediction;
import com.maxjthomas.dumpster.Stage;
import com.maxjthomas.dumpster.Type;

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
    List<Document> docList = this.ingestDocuments(nDocs);

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

  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.RebarReader#getAnnotatedDocuments(com.maxjthomas.dumpster.Stage)}.
   * 
   * @throws Exception
   * @throws RebarException
   */
  @Test
  public void testGetAnnotatedDocumentsMultiStage() throws RebarException, Exception {
    // create the stages
    Stage stageA = new Stage("stage_max_lid_test", "Testing stage for LID", Util.getCurrentUnixTime(), new HashSet<String>(), Type.LANG_ID);
    Set<String> stageBDeps = new HashSet<>();
    stageBDeps.add(stageA.name);
    Stage stageB = new Stage("stage_max_lp_test", "Testing stage for LP", Util.getCurrentUnixTime(), stageBDeps, Type.LANG_PRED);

    // ingest documents
    int nDocs = 3;
    List<Document> docList = this.ingestDocuments(nDocs);

    // annotate documents
    try (RebarAnnotator ra = new RebarAnnotator(this.conn);) {
      for (Document d : docList) {
        LangId mockLid = generateLangId(d);
        ra.addLanguageId(d, stageA, mockLid);
      }
    }

    Set<Document> fetchedDocs = this.rr.getAnnotatedDocuments(stageA);

    Set<Document> docsWithLid = new HashSet<>();
    List<LanguagePrediction> lpList = new ArrayList<>();
    try (RebarAnnotator ra = new RebarAnnotator(this.conn);) {
      // add language prediction annotation
      for (Document d : fetchedDocs) {
        LangId lid = d.lid;
        LanguagePrediction lp = new LanguagePrediction();
        Entry<String, Double> e = lid.languageToProbabilityMap.entrySet().iterator().next();

        switch (e.getKey()) {
        case "eng":
          lp.predictedLanguage = "English";
          break;
        case "fra":
          lp.predictedLanguage = "French";
          break;
        case "spa":
          lp.predictedLanguage = "Spanish";
          break;
        default:
          lp.predictedLanguage = "Unknown";
        }

        lpList.add(lp);
        ra.addLanguagePrediction(d, stageB, lp);
        Document newDoc = new Document(d);
        newDoc.language = lp;
        docsWithLid.add(newDoc);
      }
    }
    
    int annotatedDocs = 0;
    try (AccumuloStageHandler ash = new AccumuloStageHandler(this.conn);) {
      annotatedDocs = ash.getAnnotatedDocumentCount(stageB);
    }

    assertEquals("Should get n annotated docs: (n = " + nDocs + ")", nDocs, annotatedDocs);

    fetchedDocs = this.rr.getAnnotatedDocuments(stageB);
    assertEquals("Documents with LID should be the same.", docsWithLid, fetchedDocs);
  }
}

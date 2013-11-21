/**
 * 
 */
package edu.jhu.hlt.rebar.services;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.LangId;
import edu.jhu.hlt.concrete.Reader;
import edu.jhu.hlt.concrete.Stage;
import edu.jhu.hlt.rebar.accumulo.RebarAnnotator;
import edu.jhu.hlt.rebar.accumulo.RebarReader;
import edu.jhu.hlt.rebar.accumulo.TestRebarStageHandler;

/**
 * @author max
 * 
 */
public class TestRebarReaderServer extends AbstractServiceTest {

  private RebarReaderServer rrs;
  private Reader.Client client;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.rrs = new RebarReaderServer(this.conn, 9990);
    this.serviceThread = new Thread(rrs);
    this.serviceThread.setDaemon(true);
    this.serviceThread.start();

    this.initializeServiceFields();
    this.client = new Reader.Client(this.protocol);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    this.xport.close();
    this.rrs.close();
  }

  @Test
  public void canReadComms() throws Exception {
    int nComms = 10;
    List<Communication> commList = ingestDocuments(nComms);
    Set<Communication> commSet = new HashSet<Communication>(commList);

    // this.client.
    Stage newStage = TestRebarStageHandler.generateTestStage();
    List<LangId> lidList = new ArrayList<>();
    try (RebarAnnotator ra = new RebarAnnotator(this.conn);) {
      for (Communication d : commSet) {
        LangId lid = generateLangId(d);
        lidList.add(lid);
        ra.addLanguageId(d, newStage, lid);
        d.lid = lid;
      }
    }
    
    Set<Communication> apiComms;
    try (RebarReader rr = new RebarReader(this.conn);) {
      apiComms = rr.getAnnotatedCommunications(newStage);
    }
    
    Set<Communication> cliComms = this.client.getAnnotatedCommunications(newStage);
    assertEquals("Two comm sets should be equal.", apiComms, cliComms);
  }

}

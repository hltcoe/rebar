package edu.jhu.hlt.rebar.ingesters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.ballast.tools.SingleSectionSegmenter;
import edu.jhu.hlt.concrete.AnnotationMetadata;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.validation.ValidatableMetadata;
import edu.jhu.hlt.grommet.Stage;
import edu.jhu.hlt.grommet.StageType;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;
import edu.jhu.hlt.rebar.accumulo.AbstractAccumuloTest;
import edu.jhu.hlt.rebar.accumulo.CommunicationReader;
import edu.jhu.hlt.rebar.stage.AbstractStageReader;
import edu.jhu.hlt.rebar.stage.StageCreator;
import edu.jhu.hlt.rebar.stage.StageReader;

public class SectionIngesterTest extends AbstractAccumuloTest {

  SingleSectionSegmenter sss;
  
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.sss = new SingleSectionSegmenter();
  }
  
  @Test
  public void testIngestAndAnnotate() throws RebarException, Exception {
    int nDocs = 3;
    Set<Communication> commSet = generateMockDocumentSet(nDocs);
    Map<String, Communication> idToCommMap = new HashMap<>(nDocs + 1);
    List<Communication> commList = new ArrayList<>(commSet);
    for (Communication c : commList)
      idToCommMap.put(c.id, c);
    
    Map<String, SectionSegmentation> idToSSMap = new HashMap<>(11);
    // Add section segmentation to each comm.
    for (Communication c : commSet) {
      SectionSegmentation viaTool = this.sss.annotateDiff(c);
      idToSSMap.put(viaTool.uuid, viaTool);
      c.addToSectionSegmentations(viaTool);
    }
    
    Communication sample = commSet.iterator().next();
    AnnotationMetadata am = sample.getSectionSegmentationsIterator().next().getMetadata();
    assertTrue("Metadata must be valid.", new ValidatableMetadata(am).validate(sample));
    
    Stage s = new Stage()
      .setType(StageType.SECTION)
      .setName(am.getTool())
      .setDescription("Rebar test stage.")
      .setCreateTime(am.getTimestamp())
      .setDependencies(new HashSet<String>());
    
    try (StageCreator sc = new StageCreator(this.conn)) {
      sc.create(s);
    }
    
    try (SectionIngester sc = new SectionIngester(conn, s)) {
      for (Communication c : commSet) {
        sc.ingestAndAnnotate(c, s);
      }
    }
    
    CommunicationReader cr = new CommunicationReader(this.conn);
    Iterator<Communication> commIter = cr.getCommunications("Tweet");
    assertEquals("Should get " + nDocs + "ingested docs.", nDocs, Util.countIteratorResults(commIter));
    
    StageReader sr = new StageReader(this.conn);
    AbstractStageReader reader = sr.getSectionStageReader(s.name);
    Iterator<Communication> retComms = reader.getAll();
    while(retComms.hasNext()) {
      Communication c = retComms.next();
      assertTrue(idToCommMap.containsKey(c.id));
      assertEquals(1, c.getSectionSegmentationsSize());
      SectionSegmentation retrieved = c.getSectionSegmentations().get(0);
      assertTrue(idToSSMap.containsKey(retrieved.uuid));
    }
  }
}

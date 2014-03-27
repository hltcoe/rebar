/**
 * 
 */
package edu.jhu.hlt.rebar.ballast.tools;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.SentenceSegmentation;
import edu.jhu.hlt.concrete.SituationMention;
import edu.jhu.hlt.concrete.SituationMentionSet;
import edu.jhu.hlt.concrete.util.ConcreteException;
import edu.jhu.hlt.concrete.util.SuperCommunication;
import edu.jhu.hlt.rebar.ballast.AnnotationException;

/**
 * @author max
 *
 */
public class BasicSituationTaggerTest {

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  /**
   * Test method for {@link edu.jhu.hlt.rebar.ballast.tools.BasicSituationTagger#annotate(edu.jhu.hlt.concrete.Communication)}.
   * @throws IOException 
   * @throws TException 
   * @throws ConcreteException 
   * @throws AnnotationException 
   */
  @Test
  public void testAnnotate() throws IOException, TException, ConcreteException, AnnotationException {
    Communication c = new Communication();
    byte[] bytes = Files.readAllBytes(Paths.get("src/test/resources" + File.separator + "130596359.concrete"));
    new TDeserializer(new TBinaryProtocol.Factory()).deserialize(c, bytes);
    
    SuperCommunication sc = new SuperCommunication(c);
    SentenceSegmentation firstSentSeg = sc.firstSentenceSegmentation();
    String idOne = firstSentSeg.getSentenceList().get(0).getTokenizationList().get(0).getUuid();
    String idTwo = firstSentSeg.getSentenceList().get(1).getTokenizationList().get(0).getUuid();
    
    SituationMentionSet ems = new BasicSituationTagger().annotate(c);
    List<SituationMention> emList = ems.getMentionList();
    assertEquals(2, emList.size());
    
    SituationMention first = emList.get(0);
    assertEquals(idOne, first.getTokens().getTokenizationId());
    assertEquals(Integer.valueOf(3), first.getTokens().getTokenIndexList().get(0));
    
    SituationMention second = emList.get(1);
    assertEquals(idTwo, second.getTokens().getTokenizationId());
  }
}

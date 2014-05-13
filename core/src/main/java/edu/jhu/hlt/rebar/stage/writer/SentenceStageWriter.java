/**
 * 
 */
package edu.jhu.hlt.rebar.stage.writer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.SentenceSegmentation;
import edu.jhu.hlt.concrete.SentenceSegmentationCollection;
import edu.jhu.hlt.grommet.Stage;
import edu.jhu.hlt.rebar.AnnotationException;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.annotations.AbstractRebarAnnotation;
import edu.jhu.hlt.rebar.stage.AbstractStageWriter;

/**
 * @author max
 *
 */
public class SentenceStageWriter extends AbstractStageWriter<SentenceSegmentationCollection> {

  /**
   * @param conn
   * @param stage
   * @throws RebarException
   */
  public SentenceStageWriter(Connector conn, Stage stage) throws RebarException {
    super(conn, stage);
  }
  
  public SentenceStageWriter(Stage stage) throws RebarException {
    this(Constants.getConnector(), stage);
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.stages.AbstractStage#annotate(org.apache.thrift.TBase, java.lang.String)
   */
  @Override
  public void annotate(SentenceSegmentationCollection annotation, String docId) throws RebarException, AnnotationException {
    /*
     * Need to check, at minimum:
     * # Sections == # SentSegs
     * SentenceSeg.sectionId == an existing Section
     */
    AbstractRebarAnnotation<SentenceSegmentationCollection> rss = new AbstractRebarAnnotation<SentenceSegmentationCollection>(annotation) {

      /*
       * (non-Javadoc)
       * @see edu.jhu.hlt.rebar.annotations.AbstractRebarAnnotation#validate(edu.jhu.hlt.concrete.Communication)
       */
      @Override
      public boolean validate(Communication c) throws RebarException {
        int sentCollLen = this.annotation.getSentSegListSize();

        List<SectionSegmentation> sectSegList = c.getSectionSegmentations();
        if (sectSegList != null && sectSegList.size() == sentCollLen) {
          // Map from UUID --> Section
          Map<String, Section> idToSectionSegMap = new HashMap<String, Section>(sentCollLen);
          
          for (SectionSegmentation ss : sectSegList) {
            for (Section s : ss.getSectionList())
              idToSectionSegMap.put(s.uuid, s);
            
            for (SentenceSegmentation sts : this.annotation.getSentSegList()) {
              if (!idToSectionSegMap.containsKey(sts.sectionId))
                return false;
            }
          }
        }
        
        return true;
      }
    };
    
    this.annotate(rss, docId);
  }
}

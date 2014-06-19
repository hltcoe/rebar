/**
 * 
 */
package edu.jhu.hlt.rebar.stage.writer;

import org.apache.accumulo.core.client.Connector;

import edu.jhu.hlt.concrete.SentenceSegmentationCollection;
import edu.jhu.hlt.concrete.validation.AbstractAnnotation;
import edu.jhu.hlt.concrete.validation.ValidatableSentenceSegmentationCollection;
import edu.jhu.hlt.rebar.AnnotationException;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.stage.AbstractStageWriter;
import edu.jhu.hlt.rebar.stage.Stage;

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
    AbstractAnnotation<SentenceSegmentationCollection> rss = new ValidatableSentenceSegmentationCollection(annotation);
    this.annotate(rss, docId);
  }
}

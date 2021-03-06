/**
 * 
 */
package edu.jhu.hlt.rebar.ingesters;

import org.apache.accumulo.core.client.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.concrete.AnnotationMetadata;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.communications.SuperCommunication;
import edu.jhu.hlt.concrete.util.ConcreteException;
import edu.jhu.hlt.concrete.validation.ValidatableMetadata;
import edu.jhu.hlt.concrete.validation.ValidatableSectionSegmentation;
import edu.jhu.hlt.rebar.AnnotationException;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.accumulo.CleanIngester;
import edu.jhu.hlt.rebar.accumulo.CommunicationReader;
import edu.jhu.hlt.rebar.stage.Stage;
import edu.jhu.hlt.rebar.stage.StageCreator;
import edu.jhu.hlt.rebar.stage.StageType;
import edu.jhu.hlt.rebar.stage.writer.SectionStageWriter;

/**
 * @author max
 *
 */
public class SectionIngester implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(SectionIngester.class);
  
  protected final Connector conn;
  protected final CleanIngester ci;
  protected final StageCreator sc;
  protected final Stage s;
  protected final SectionStageWriter sectionWriter;
  
  /**
   * @throws RebarException 
   * 
   */
  public SectionIngester(Connector conn, Stage s) throws RebarException {
    if (s.getStageType() != StageType.SECTION)
      throw new RebarException("You can't use this ingester with a stage of type: " + s.getStageType().toString());
    this.conn = conn;
    this.ci = new CleanIngester(this.conn);
    this.sc = new StageCreator(this.conn);
    this.s = s;
    this.sectionWriter = new SectionStageWriter(this.conn, s);
  }
  
  public SectionIngester(Stage s) throws RebarException {
    this(Constants.getConnector(), s);
  }
  
  public void ingestAndAnnotate(Communication c, Stage s) throws RebarException, AnnotationException {
    try {
      SuperCommunication sc = new SuperCommunication(c);
      logger.debug("Checking for sections.");
      if (!sc.hasSections())
        throw new AnnotationException("Communication: " + c.getId() + " does not have sections.");
      SectionSegmentation ss = sc.firstSectionSegmentation();
      logger.debug("Working with SectionSegmentation object: {}", ss.toString());
      logger.debug("Attempting to validate first section segmentation object.");
      if (!new ValidatableSectionSegmentation(ss).validate(c))
        throw new AnnotationException("Section segmentation: " + ss.getUuid() + " is not valid; cannot continue.");
      AnnotationMetadata md = ss.getMetadata();
      logger.debug("Attempting to validate AnnotationMetadata object.");
      logger.debug("AnnotationMetadata: {}", md.toString());
      if (!new ValidatableMetadata(md).validate(c))
        throw new AnnotationException("Metadata for section segmentation: " + ss.getUuid() + " is not valid; cannot continue.");

      // see if comm exists. if not, ingest comm *ONLY*.
      if (!new CommunicationReader(this.conn).exists(c)) {
        logger.debug("Communication {} is not ingested; ingesting.", c.getId());
        Communication stripped = new SuperCommunication(c).stripAnnotations().getCopy();
        this.ci.ingest(stripped);
      }
      
      // ingest the annotation.
      this.sectionWriter.annotate(ss, c.getId());
    } catch (ConcreteException e) {
      throw new AnnotationException(e);
    }
  }

  @Override
  public void close() throws Exception {
    this.sectionWriter.close();
    this.sc.close();
    this.ci.close();
  }
}

/**
 * 
 */
package edu.jhu.hlt.rebar.ingesters;

import org.apache.accumulo.core.client.Connector;

import edu.jhu.hlt.concrete.AnnotationMetadata;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.communications.SuperCommunication;
import edu.jhu.hlt.concrete.util.ConcreteException;
import edu.jhu.hlt.concrete.validation.ValidatableMetadata;
import edu.jhu.hlt.concrete.validation.ValidatableSectionSegmentation;
import edu.jhu.hlt.grommet.Stage;
import edu.jhu.hlt.rebar.AnnotationException;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.accumulo.CleanIngester;
import edu.jhu.hlt.rebar.stage.StageCreator;
import edu.jhu.hlt.rebar.stage.writer.SectionStageWriter;

/**
 * @author max
 *
 */
public class SectionIngester implements AutoCloseable {

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
      if (!sc.hasSections())
        throw new AnnotationException("Communication: " + c.getId() + " does not have sections.");
      SectionSegmentation ss = sc.firstSectionSegmentation();
      if (!new ValidatableSectionSegmentation(ss).validate(c))
        throw new AnnotationException("Section segmentation: " + ss.getUuid() + " is not valid; cannot continue.");
      AnnotationMetadata md = ss.getMetadata();
      if (!new ValidatableMetadata(md).validate(c))
        throw new AnnotationException("Metadata for section segmentation: " + ss.getUuid() + " is not valid; cannot continue.");

      // ingest the comm *ONLY*.
      Communication stripped = new SuperCommunication(c).stripAnnotations().getCopy();
      this.ci.ingest(stripped);
      
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

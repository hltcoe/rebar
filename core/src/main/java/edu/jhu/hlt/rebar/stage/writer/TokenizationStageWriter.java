/**
 * 
 */
package edu.jhu.hlt.rebar.stage.writer;

import org.apache.accumulo.core.client.Connector;

import edu.jhu.hlt.concrete.TokenizationCollection;
import edu.jhu.hlt.concrete.validation.AbstractAnnotation;
import edu.jhu.hlt.concrete.validation.ValidatableTokenizationCollection;
import edu.jhu.hlt.rebar.AnnotationException;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.stage.AbstractStageWriter;
import edu.jhu.hlt.rebar.stage.Stage;

/**
 * @author max
 *
 */
public class TokenizationStageWriter extends AbstractStageWriter<TokenizationCollection> {

  /**
   * @param conn
   * @param stage
   * @throws RebarException
   */
  public TokenizationStageWriter(Connector conn, Stage stage) throws RebarException {
    super(conn, stage);
  }
  
  public TokenizationStageWriter(Stage stage) throws RebarException {
    this(Constants.getConnector(), stage);
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.stages.AbstractStage#annotate(org.apache.thrift.TBase, java.lang.String)
   */
  @Override
  public void annotate(TokenizationCollection annotation, String docId) throws RebarException, AnnotationException {
    /*
     * TODO: switch to screed, or some other legit validation strategy.
     */
    AbstractAnnotation<TokenizationCollection> rss = new ValidatableTokenizationCollection(annotation);
    this.annotate(rss, docId);
  }
}

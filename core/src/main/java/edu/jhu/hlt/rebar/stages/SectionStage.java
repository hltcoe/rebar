/**
 * 
 */
package edu.jhu.hlt.rebar.stages;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;

import edu.jhu.hlt.asphalt.Stage;
import edu.jhu.hlt.asphalt.StageType;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.rebar.AnnotationException;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.accumulo.AbstractCommunicationWriter;
import edu.jhu.hlt.rebar.accumulo.CommunicationReader;
import edu.jhu.hlt.rebar.annotations.IValidatable;
import edu.jhu.hlt.rebar.annotations.RebarSectionSegmentation;

/**
 * @author max
 *
 */
public class SectionStage extends AbstractCommunicationWriter {

  private final Stage stage;
  private final CommunicationReader reader;
  
  public SectionStage(Stage s) throws RebarException {
    this(Constants.getConnector(), s);
  }
  
  /**
   * 
   */
  public SectionStage(Connector conn, Stage s) throws RebarException {
    super(conn);
    
    if (s.type == StageType.SECTION) {
      this.stage = s;
    } else {
      throw new RebarException("Cannot instantiate a SectionStage with stage of type: " + s.type.toString());
    }
    
    this.reader = new CommunicationReader(this.conn);
  }
  
  public void annotate(SectionSegmentation ss, String docId) throws RebarException, AnnotationException {
    try {
      Communication c = this.reader.get(docId);
      RebarSectionSegmentation rss = new RebarSectionSegmentation(ss);
      if (rss.validate(c)) {
        Mutation m = new Mutation(c.uuid);
        m.put("annotations", this.stage.name, new Value(this.serializer.serialize(ss)));
        this.bw.addMutation(m);
      } else {
        throw new AnnotationException("Your SectionSegmentation object was invalid.");
      }
    } catch (TException | MutationsRejectedException e) {
      throw new RebarException(e);
    }
  }
}

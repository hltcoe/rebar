/**
 * 
 */
package edu.jhu.hlt.rebar.stages;

import java.util.Iterator;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;

import edu.jhu.hlt.asphalt.Stage;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.rebar.AnnotationException;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;
import edu.jhu.hlt.rebar.annotations.AbstractRebarAnnotation;

/**
 * @author max
 *
 */
public class SectionStage extends AbstractStage<SectionSegmentation> {
  
  public SectionStage(Connector conn, Stage stage) throws RebarException {
    super(conn, stage);
  }

  public SectionStage(Stage stage) throws RebarException {
    this(Constants.getConnector(), stage);
  }

  public void annotate(SectionSegmentation ss, String docId) throws RebarException, AnnotationException {
    try {
      Communication c = this.reader.get(docId);
      AbstractRebarAnnotation<SectionSegmentation> rss = new AbstractRebarAnnotation<SectionSegmentation>(ss) {

        @Override
        public boolean validate(Communication c) throws RebarException {
          boolean valid = 
              this.annotation.metadata != null
              && Util.isValidUUIDString(this.annotation.uuid)
              && this.annotation.isSetSectionList();
          Iterator<Section> sects = this.annotation.getSectionListIterator();
          while (valid && sects.hasNext()) {
            Section s = sects.next();
            valid = 
                Util.isValidUUIDString(s.uuid);
          }
          
          return valid;
        }
      };
      
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

/**
 * 
 */
package edu.jhu.hlt.rebar.annotations;

import java.util.Iterator;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.rebar.AnnotationException;
import edu.jhu.hlt.rebar.Util;

/**
 * @author max
 *
 */
public class RebarSectionSegmentation extends AbstractRebarAnnotation<SectionSegmentation> {

  /**
   * 
   */
  public RebarSectionSegmentation(SectionSegmentation annot) throws AnnotationException {
    super(annot);
  }

  @Override
  public boolean validate(Communication c) {
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
}

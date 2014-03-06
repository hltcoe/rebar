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
public class RebarSectionSegmentation implements IValidatable {

  private final SectionSegmentation annot;
  
  /**
   * 
   */
  public RebarSectionSegmentation(SectionSegmentation annot) throws AnnotationException {
    this.annot = annot;
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.annotations.IValidatable#validate(edu.jhu.hlt.concrete.Communication)
   */
  @Override
  public boolean validate(Communication c) {
    boolean valid = 
        this.annot.metadata != null
        && Util.isValidUUIDString(this.annot.uuid)
        && this.annot.isSetSectionList();
    Iterator<Section> sects = this.annot.getSectionListIterator();
    while (valid && sects.hasNext()) {
      Section s = sects.next();
      valid = 
          Util.isValidUUIDString(s.uuid);
    }
    
    return valid;
  }
}

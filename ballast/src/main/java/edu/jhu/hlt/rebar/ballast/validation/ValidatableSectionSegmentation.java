/**
 * 
 */
package edu.jhu.hlt.rebar.ballast.validation;

import java.util.Iterator;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.rebar.ballast.Util;

/**
 * @author max
 *
 */
public class ValidatableSectionSegmentation extends AbstractAnnotation<SectionSegmentation> {

  /**
   * 
   */
  public ValidatableSectionSegmentation(SectionSegmentation annot) {
    super(annot);
  }

  /*
   * (non-Javadoc)
   * @see edu.jhu.hlt.rebar.ballast.validation.AbstractAnnotation#isValid(edu.jhu.hlt.concrete.Communication)
   */
  @Override
  public boolean isValid(Communication c) {
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

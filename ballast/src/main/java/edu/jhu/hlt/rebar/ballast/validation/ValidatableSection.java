/**
 * 
 */
package edu.jhu.hlt.rebar.ballast.validation;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Section;

/**
 * @author max
 *
 */
public class ValidatableSection extends AbstractAnnotation<Section> {

  /**
   * @param annotation
   */
  public ValidatableSection(Section annotation) {
    super(annotation);
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.ballast.validation.AbstractAnnotation#isValid(edu.jhu.hlt.concrete.Communication)
   */
  @Override
  public boolean isValid(Communication c) {
    return false;
  }
}

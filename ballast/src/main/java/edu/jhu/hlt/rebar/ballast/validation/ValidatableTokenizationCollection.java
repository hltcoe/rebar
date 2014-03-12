/**
 * 
 */
package edu.jhu.hlt.rebar.ballast.validation;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.TokenizationCollection;

/**
 * @author max
 *
 */
public class ValidatableTokenizationCollection extends AbstractAnnotation<TokenizationCollection> {


  public ValidatableTokenizationCollection(TokenizationCollection annotation) {
    super(annotation);
  }

  /*
   * (non-Javadoc)
   * @see edu.jhu.hlt.rebar.ballast.validation.AbstractAnnotation#isValid(edu.jhu.hlt.concrete.Communication)
   */
  @Override
  public boolean isValid(Communication c) {
    return true;
  }
}

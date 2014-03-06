/**
 * 
 */
package edu.jhu.hlt.rebar.annotations;

import edu.jhu.hlt.concrete.Communication;

/**
 * @author max
 *
 */
public interface IValidatable {
  public boolean validate(Communication c);
}

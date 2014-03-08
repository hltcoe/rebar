/**
 * 
 */
package edu.jhu.hlt.rebar.annotations;

import edu.jhu.hlt.concrete.Communication;

/**
 * @author max
 *
 */
public interface IValidatable<T> {
  public boolean validate(T annotation, Communication c);
}

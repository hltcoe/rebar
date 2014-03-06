/**
 * 
 */
package edu.jhu.hlt.rebar;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.annotations.IValidatable;

/**
 * @author max
 *
 */
public class RebarAnnotation<T> implements IValidatable {
  
  

  @Override
  public boolean validate(Communication c) {
    
    return false;
  }
}

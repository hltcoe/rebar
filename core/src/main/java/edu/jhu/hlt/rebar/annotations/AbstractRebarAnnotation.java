/**
 * 
 */
package edu.jhu.hlt.rebar.annotations;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.RebarException;

/**
 * TODO: type bounding on T.
 * 
 * @author max
 */
public abstract class AbstractRebarAnnotation<T> {

  protected final T annotation;
  
  /**
   * 
   */
  public AbstractRebarAnnotation(T annotation) {
    this.annotation = annotation;
  }

  public abstract boolean validate(Communication c) throws RebarException;
  
  public T getAnnotation() {
    return this.annotation;
  }
}

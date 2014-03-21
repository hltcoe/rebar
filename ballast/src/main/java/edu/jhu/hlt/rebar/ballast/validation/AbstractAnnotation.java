/**
 * 
 */
package edu.jhu.hlt.rebar.ballast.validation;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import edu.jhu.hlt.concrete.Communication;

/**
 * 
 * @author max
 */
public abstract class AbstractAnnotation<T extends TBase<T, ? extends TFieldIdEnum>> {

  protected final T annotation;
  
  /**
   * 
   */
  public AbstractAnnotation(T annotation) {
    this.annotation = annotation;
  }

  /**
   * Clients should implement this method, which will be called by Rebar to 
   * attempt to validate the annotation object.
   * 
   * @param c - A communication associated with the {@link AbstractAnnotation}.
   * @return true if valid.
   */
  public abstract boolean isValid(Communication c);
  
  /**
   * Get the annotation. 
   */
  public T getAnnotation() {
    return this.annotation;
  }
}

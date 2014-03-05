/**
 * 
 */
package edu.jhu.hlt.rebar;

/**
 * @author max
 *
 */
public class AnnotationException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = -514171687897685624L;

  /**
   * 
   */
  public AnnotationException() {
    this("There was an exception creating an annotation.");
  }

  /**
   * @param message
   */
  public AnnotationException(String message) {
    super(message);
  }

  /**
   * @param cause
   */
  public AnnotationException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public AnnotationException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * @param message
   * @param cause
   * @param enableSuppression
   * @param writableStackTrace
   */
  public AnnotationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}

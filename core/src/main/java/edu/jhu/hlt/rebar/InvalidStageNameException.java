/*
 * 
 */
package edu.jhu.hlt.rebar;

/**
 * @author max
 *
 */
public class InvalidStageNameException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  /**
   * @param stageName
   */
  public InvalidStageNameException(String stageName) {
    super("Stage name was not valid: " + stageName);
  }
}

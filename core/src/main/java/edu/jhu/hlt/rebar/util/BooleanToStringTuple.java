package edu.jhu.hlt.rebar.util;

/**
 * A laughable class to represent a boolean-string tuple.
 * 
 * @author max
 */
public class BooleanToStringTuple {
  private final boolean bool;
  private final String message;
  
  public BooleanToStringTuple(boolean b, String s) {
    this.bool = b;
    this.message = s;
  }
  
  public boolean getBoolean() {
    return this.bool;
  }
  
  public String getMessage() {
    return this.message;
  }
}
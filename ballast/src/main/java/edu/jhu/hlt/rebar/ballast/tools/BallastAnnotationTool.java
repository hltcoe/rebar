/**
 * 
 */
package edu.jhu.hlt.rebar.ballast.tools;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import edu.jhu.hlt.concrete.AnnotationMetadata;
import edu.jhu.hlt.rebar.ballast.AnnotationTool;

/**
 * Sample abstract class for Ballast- (example) based tools.
 * 
 * @author max
 */
abstract class BallastAnnotationTool<T extends TBase<T, ? extends TFieldIdEnum>> implements AnnotationTool<T> {

  /**
   * 
   */
  public BallastAnnotationTool() {
    // TODO Auto-generated constructor stub
  }

  /**
   * Generate an {@link AnnotationMetadata} object that describes this "tool".
   *
   * @return a {@link AnnotationMetadata} object for this project
   */
  @Override
  public final AnnotationMetadata getMetadata() {
    AnnotationMetadata md = new AnnotationMetadata();
    md.confidence = 0.0d;
    md.timestamp = (int) (System.currentTimeMillis() / 1000);
    md.tool = "concrete-examples";
    return md;
  }
}

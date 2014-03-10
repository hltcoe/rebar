/**
 * 
 */
package edu.jhu.hlt.rebar.annotations;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.rebar.AnnotationException;

/**
 * @author max
 *
 */
public interface SectionSegmenter {
  public SectionSegmentation section(Communication comm) throws AnnotationException;
}

/*
 * Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
 * See LICENSE in the project root directory.
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

  }

  /**
   * Generate an {@link AnnotationMetadata} object that describes this "tool".
   *
   * @return a {@link AnnotationMetadata} object for this project
   */
  /*
   * (non-Javadoc)
   * @see edu.jhu.hlt.rebar.ballast.AnnotationTool#getMetadata()
   */
  @Override
  public final AnnotationMetadata getMetadata() {
    AnnotationMetadata md = new AnnotationMetadata();
    md.confidence = 0.0d;
    md.timestamp = (System.currentTimeMillis() / 1000);
    md.tool = "concrete-examples";
    return md;
  }
}

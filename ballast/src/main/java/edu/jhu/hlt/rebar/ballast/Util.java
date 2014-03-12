/*
 * Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */
package edu.jhu.hlt.rebar.ballast;

import java.util.UUID;

import edu.jhu.hlt.concrete.AnnotationMetadata;

/**
 * @author max
 *
 */
public final class Util {

  /**
   * Generate an {@link AnnotationMetadata} object that describes this "tool".
   *
   * @return a {@link AnnotationMetadata} object for this project
   */
  public static AnnotationMetadata getMetadata() {
    AnnotationMetadata md = new AnnotationMetadata();
    md.confidence = 1.0d;
    md.timestamp = (int) (System.currentTimeMillis() / 1000);
    md.tool = "rebar-ballast";
    return md;
  }
  
  public static boolean isValidUUIDString(String uuidStr) {
    try {
      UUID.fromString(uuidStr);
      return true;
    } catch (IllegalArgumentException iae) {
      return false;
    }
  }
}

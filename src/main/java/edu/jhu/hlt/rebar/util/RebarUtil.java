/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */
package edu.jhu.hlt.rebar.util;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * @author max
 *
 */
public class RebarUtil {

  public static final Value EMPTY_VALUE = new Value(new byte[0]);
  
  /**
   * 
   */
  private RebarUtil() {
    // TODO Auto-generated constructor stub
  }

  public static int countIteratorResults(Iterator<Entry<Key, Value>> iter) {
    int resCt = 0;
    while (iter.hasNext()) {
      iter.next();
      resCt++;
    }
    
    return resCt;
  }
}

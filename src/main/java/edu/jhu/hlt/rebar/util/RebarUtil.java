/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */
package edu.jhu.hlt.rebar.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.maxjthomas.dumpster.RebarThriftException;

import edu.jhu.hlt.rebar.RebarException;

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

  public static RebarThriftException wrapException(Exception e) throws RebarException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try {
      new ObjectOutputStream(os).writeObject(e);
    } catch (IOException e1) {
      throw new RebarException(e1.getMessage());
    }

    RebarThriftException ie = new RebarThriftException(e.getMessage());
    ie.setSerEx(os.toByteArray());
    return ie;
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

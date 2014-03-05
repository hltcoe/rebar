/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */
package edu.jhu.hlt.rebar;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.joda.time.DateTime;
import org.slf4j.Logger;

/**
 * @author max
 *
 */
public class Util {

  public static final Value EMPTY_VALUE = new Value(new byte[0]);
  
  /**
   * 
   */
  private Util() {
    // TODO Auto-generated constructor stub
  }
  
  public static int getCurrentUnixTime() {
    long millis = new DateTime().getMillis();
    return ((int) (millis/1000));
  }

  public static int countIteratorResults(Iterator<Entry<Key, Value>> iter) {
    int resCt = 0;
    while (iter.hasNext()) {
      iter.next();
      resCt++;
    }
    
    return resCt;
  }

  public static void printTable(String tableName, Connector conn, Logger logger) throws RebarException {
    try {
      Scanner sc = conn.createScanner(tableName, Configuration.getAuths());
      Iterator<Entry<Key, Value>> iter = sc.iterator();
      while(iter.hasNext())
        logger.info(iter.next().getKey().toString());
      
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }

  public static Mutation generateEmptyValueMutation(String rowId, String colF, String colQ) {
    final Mutation m = new Mutation(rowId);
    m.put(colF, colF, Constants.EMPTY_VALUE);
    return m;
  }
}

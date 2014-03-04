/**
 * 
 */
package edu.jhu.hlt.rebar.util;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;

import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public class RebarUtil {

  /**
   * 
   */
  private RebarUtil() {
    // TODO Auto-generated constructor stub
  }

  public static Mutation generateEmptyValueMutation(String rowId, String colF, String colQ) {
    final Mutation m = new Mutation(rowId);
    m.put(colF, colF, Constants.EMPTY_VALUE);
    return m;
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
}

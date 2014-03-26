/**
 * 
 */
package edu.jhu.hlt.rebar.services;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.asphalt.services.Ingester;
import edu.jhu.hlt.asphalt.AsphaltException;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;
import edu.jhu.hlt.rebar.accumulo.AbstractAccumuloTest;

/**
 * @author max
 *
 */
public class TestRebarIngesterServer extends AbstractServiceTest {

  private RebarIngesterServer ris;
  private Ingester.Client client;
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.ris = new RebarIngesterServer(this.conn, 9990);
    this.serviceThread = new Thread(ris);
    this.serviceThread.setDaemon(true);
    this.serviceThread.start();
    
    this.initializeServiceFields();
    this.client = new Ingester.Client(this.protocol);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    this.xport.close();
    this.ris.close();
  }
  

//  @Test
//  public void testBulkInsertCommunications() throws AsphaltException, TException, TableNotFoundException, RebarException {
//    int nComms = 10;
//    Set<Communication> commSet = AbstractAccumuloTest.generateMockDocumentSet(nComms);
//    nComms = commSet.size(); // in case there were duplicate IDs.
//    for (Communication c : commSet) {
//      //logger.info("Ingesting comm: " + c.id);
//      this.client.ingest(c);
//    }
//    
//    Iterator<Entry<Key, Value>> iterator = AbstractAccumuloTest.generateIterator(this.conn, Constants.DOCUMENT_TABLE_NAME, new Range());
//    assertEquals("Should find " + nComms + " docs.", nComms, Util.countIteratorResults(iterator));
//  }
}

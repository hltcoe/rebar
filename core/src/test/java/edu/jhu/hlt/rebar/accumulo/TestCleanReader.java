/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.CommunicationType;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public class TestCleanReader extends AbstractAccumuloTest {

  private static final Logger logger = LoggerFactory.getLogger(TestCleanReader.class);
  
  CleanIngester ci;
  CleanReader cr;
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.ci = new CleanIngester(this.conn);
    this.cr = new CleanReader(this.conn);
    
    
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.CleanReader#getCommunications(edu.jhu.hlt.concrete.CommunicationType)}.
   * 
   * @throws RebarException 
   * @throws TableNotFoundException 
   * @throws MutationsRejectedException 
   */
  @Test
  public void communicationsByType() throws RebarException, TableNotFoundException, MutationsRejectedException {
    assertFalse(cr.getCommunications(CommunicationType.NEWS).hasNext());
    Communication c = generateMockDocument();
    ci.ingest(c);
    Communication c2 = generateMockDocument();
    ci.ingest(c2);
    
    assertFalse(cr.getCommunications(CommunicationType.NEWS).hasNext());
    assertTrue(cr.getCommunications(CommunicationType.TWEET).hasNext());
    
    Iterator<Communication> commIter = cr.getCommunications(CommunicationType.TWEET);
    int ct = 0;
    while (commIter.hasNext()) {
      commIter.next();
      ct++;
    }
    
    assertEquals(2, ct);
  }
  
  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.CleanReader#getCommunications(DateTime, DateTime)}.
   * 
   * @throws RebarException 
   * @throws TableNotFoundException 
   * @throws MutationsRejectedException 
   */
  @Test
  public void communicationsByDate() throws RebarException, TableNotFoundException, MutationsRejectedException {
    assertFalse(cr.getCommunications(new DateTime(2014, 1, 1, 1, 1), new DateTime()).hasNext());
    Communication c = generateMockDocument();
    c.startTime = 39595830;
    ci.ingest(c);
    assertFalse(cr.getCommunications(new DateTime(2014, 1, 1, 1, 1), new DateTime()).hasNext());
    assertTrue(cr.getCommunications(new DateTime(1970, 1, 1, 1, 1), new DateTime()).hasNext());
    Communication c2 = generateMockDocument();
    ci.ingest(c2);
    
    Iterator<Communication> commIter = cr.getCommunications(new DateTime(1970, 1, 1, 1, 1), new DateTime());
    int ct = 0;
    while (commIter.hasNext()) {
      commIter.next();
      ct++;
    }
    
    assertEquals(1, ct);
  }
}

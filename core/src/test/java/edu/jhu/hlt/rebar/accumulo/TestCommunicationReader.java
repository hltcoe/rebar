/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.UUID;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public class TestCommunicationReader extends AbstractAccumuloTest {

  private static final Logger logger = LoggerFactory.getLogger(TestCommunicationReader.class);
  
  CleanIngester ci;
  CommunicationReader cr;
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.ci = new CleanIngester(this.conn);
    this.cr = new CommunicationReader(this.conn);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    this.ci.close();
  }
  
  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.CommunicationReader#byUuid(String)}.
   * 
   * @throws RebarException 
   */
  @Test(expected=RebarException.class)
  public void byUuidNonExistent() throws RebarException {
    Communication c = generateMockDocument();
    ci.ingest(c);
    
    this.cr.byUuid(UUID.randomUUID());
  }
  
  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.CommunicationReader#byUuid(String)}.
   * 
   * @throws RebarException 
   */
  @Test
  public void byUuid() throws RebarException {
    Communication c = generateMockDocument();
    UUID id = UUID.fromString(c.uuid);
    ci.ingest(c);
    for (int i = 0; i < 100; i++)
      ci.ingest(generateMockDocument());
    
    assertEquals(c, this.cr.byUuid(id));
  }
  
  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.CommunicationReader#get(String)}.
   * 
   * @throws RebarException 
   */
  @Test
  public void get() throws RebarException {
    Communication toCheck = generateMockDocument();

    for (int i = 0; i < 100; i++)
      ci.ingest(generateMockDocument());
    ci.ingest(toCheck);
    
    assertEquals(toCheck, this.cr.get(toCheck.id));
  }
  
  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.CommunicationReader#get(String)}.
   * 
   * @throws RebarException 
   */
  @Test(expected=RebarException.class)
  public void getNonExistent() throws RebarException {
    Communication c = generateMockDocument();
    ci.ingest(c);
    for (int i = 0; i < 100; i++)
      ci.ingest(generateMockDocument());
    
    this.cr.get("foo");
  }

  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.CommunicationReader#getCommunications(String)}.
   * 
   * @throws RebarException 
   * @throws TableNotFoundException 
   * @throws MutationsRejectedException 
   */
  @Test
  public void communicationsByType() throws RebarException, TableNotFoundException, MutationsRejectedException {
    assertFalse(cr.getCommunications("News").hasNext());
    Communication c = generateMockDocument();
    ci.ingest(c);
    Communication c2 = generateMockDocument();
    ci.ingest(c2);
    
    assertFalse(cr.getCommunications("News").hasNext());
    assertTrue(cr.getCommunications("Tweet").hasNext());
    
    Iterator<Communication> commIter = cr.getCommunications("Tweet");
    int ct = 0;
    while (commIter.hasNext()) {
      commIter.next();
      ct++;
    }
    
    assertEquals(2, ct);
    
    commIter = cr.getCommunications("Tweet");
    ct = 0;
    
    while (commIter.hasNext()) {
      commIter.next();
      ct++;
    }
    
    assertEquals(2, ct);
  }
  
  /**
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.CommunicationReader#getCommunications(DateTime, DateTime)}.
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

/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.util.RebarUtil;

/**
 * @author max
 *
 */
public class TestCleanIngester extends AbstractAccumuloTest {

  private static final Logger logger = LoggerFactory.getLogger(TestCleanIngester.class);
  
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
   * Test method for {@link edu.jhu.hlt.rebar.accumulo.CleanIngester#isDocumentIngested(edu.jhu.hlt.concrete.Communication)}.
   * 
   * @throws RebarException 
   * @throws TableNotFoundException 
   * @throws MutationsRejectedException 
   */
  @Test
  public void testIsDocumentIngested() throws RebarException, TableNotFoundException, MutationsRejectedException {
    assertFalse(cr.exists("bar"));
    Communication c = generateMockDocument();
    Communication c2 = generateMockDocument();
    c.startTime = 39595830;
    c2.startTime = 395958301;
    assertFalse(cr.exists(c));
    ci.ingest(c);
    assertTrue(cr.exists(c));
    assertFalse(cr.exists("bar"));
    ci.ingest(c);
    ci.ingest(c2);
    assertTrue(cr.exists(c));
    assertTrue(cr.exists(c2));
//    RebarUtil.printTable(Constants.DOCUMENT_IDX_TABLE, this.conn, logger);
  }
}

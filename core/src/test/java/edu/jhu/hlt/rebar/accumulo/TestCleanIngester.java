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

/**
 * @author max
 *
 */
public class TestCleanIngester extends AbstractAccumuloTest {

  private static final Logger logger = LoggerFactory.getLogger(TestCleanIngester.class);
  
  CleanIngester ci;
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.ci = new CleanIngester(this.conn);
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
    assertFalse(ci.isDocumentIngested("bar"));
    Communication c = generateMockDocument();
    assertFalse(ci.isDocumentIngested(c));
    ci.ingest(c);
    assertTrue(ci.isDocumentIngested(c));
    assertFalse(ci.isDocumentIngested("bar"));
  }
}

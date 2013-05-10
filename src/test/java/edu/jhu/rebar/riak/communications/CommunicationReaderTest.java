/**
 * 
 */
package edu.jhu.rebar.riak.communications;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.util.Set;

import org.apache.commons.lang.time.StopWatch;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.RiakException;

import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.ingest.twitter.TwitterCommunicationParser;
import edu.jhu.rebar.riak.RiakCommunication;
import edu.jhu.rebar.riak.RiakCorpus;
import edu.jhu.rebar.riak.RiakCorpusFactory;

/**
 * @author max
 *
 */
public class CommunicationReaderTest {
    
    private static final Logger logger = Logger.getLogger(CommunicationReaderTest.class);
    
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for {@link edu.jhu.rebar.riak.communications.CommunicationReader#fetchAllComms(java.lang.String)}.
     * @throws RiakException 
     * @throws RebarException 
     * @throws FileNotFoundException 
     * @throws InterruptedException 
     */
    @Test
    public void testFetchAllComms() throws RiakException, RebarException, FileNotFoundException, InterruptedException {
        // create test corpus
        String testCorpusName = "unit_test_read";
        RiakCorpusFactory rcf = new RiakCorpusFactory();
        // delete it in case it's there already, then remake it
        rcf.deleteCorpus(testCorpusName);
        RiakCorpus rc = (RiakCorpus) rcf.makeCorpus(testCorpusName);
        
        // get some tweets to ingest
        TwitterCommunicationParser tcp = new TwitterCommunicationParser(rc);
        logger.info("Reading file...");
        Set<RiakCommunication> tweetsAsComms = tcp.parseTwitterAPIFile("/media/HD3/data/twitter/03-01-11.txt.bz2");
        int sizeOfComms = tweetsAsComms.size();
        
        // ingest the tweets
        CommunicationIngester ci = new CommunicationIngester(tweetsAsComms, rc);
        StopWatch sw = new StopWatch();
        sw.start();
        
        // do actual ingest
        logger.info("Starting ingest...");
        ci.bulkIngest();
        
        sw.stop();
        double time = (double) sw.getTime() / 1000;
        logger.info("Took " + time + " seconds to ingest " + sizeOfComms + " tweets.");
        
        CommunicationReader cr = new CommunicationReader();
        sw.reset();
        sw.start();
        Set<RiakCommunication> commSet = cr.fetchAllComms(testCorpusName);
        sw.stop();
        time = (double) sw.getTime() / 1000;
        logger.info("Took " + time + " seconds to retrieve " + sizeOfComms + " tweets.");
        assertEquals(sizeOfComms, commSet.size());
        cr.close();
        
        rcf.deleteCorpus(testCorpusName);
        rcf.close();
    }

}

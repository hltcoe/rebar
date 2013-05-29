package edu.jhu.rebar.riak.itest;

import static org.junit.Assert.assertEquals;

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
import edu.jhu.rebar.riak.communications.CommunicationIngester;

import static org.mockito.Mockito.*;

public class ITestCommunicationIngester {
    
    private static final Logger logger = Logger.getLogger(ITestCommunicationIngester.class);
    
    
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testIngest() throws RiakException, RebarException, FileNotFoundException, InterruptedException {
        // create test corpus
        String testCorpusName = "unit_test_ingest";
        RiakCorpusFactory rcf = new RiakCorpusFactory();
        // delete it in case it's there already, then remake it
        rcf.deleteCorpus(testCorpusName);
        RiakCorpus rc = (RiakCorpus) rcf.makeCorpus(testCorpusName);
        
        // get some tweets to ingest
        TwitterCommunicationParser tcp = new TwitterCommunicationParser(rc);
        Set<RiakCommunication> tweetsAsComms = tcp.parseTwitterAPIFile(ITestCommunicationIngester.class
                .getClassLoader()
                .getResourceAsStream("10k-tweets.txt"));
        int sizeOfComms = tweetsAsComms.size();
        
        // ingest the tweets
        CommunicationIngester ci = new CommunicationIngester(tweetsAsComms, rc);
        StopWatch sw = new StopWatch();
        sw.start();
        
        // do actual ingest
        ci.ingest();
        
        sw.stop();
        double time = (double) sw.getTime() / 1000;
        logger.info("Took " + time + " seconds to ingest " + sizeOfComms + " tweets.");
        
        Thread.sleep(5000);
        rc = (RiakCorpus) rcf.getCorpus(testCorpusName);
        Set<String> commIds = rc.getCommIdSet();
        assertEquals(sizeOfComms, commIds.size());
        
        // delete the corpus
        rcf.deleteCorpus(testCorpusName);
        rcf.close();
    }
    
    @Test
    public void testBulkIngest() throws RiakException, RebarException, FileNotFoundException, InterruptedException {
        // create test corpus
        String testCorpusName = "unit_test_ingest";
        RiakCorpusFactory rcf = new RiakCorpusFactory();
        // delete it in case it's there already, then remake it
        rcf.deleteCorpus(testCorpusName);
        RiakCorpus rc = (RiakCorpus) rcf.makeCorpus(testCorpusName);
        
        // get some tweets to ingest
        TwitterCommunicationParser tcp = new TwitterCommunicationParser(rc);
        Set<RiakCommunication> tweetsAsComms = tcp.parseTwitterAPIFile(ITestCommunicationIngester.class
                .getClassLoader()
                .getResourceAsStream("10k-tweets.txt"));
        int sizeOfComms = tweetsAsComms.size();
        
        // ingest the tweets
        CommunicationIngester ci = new CommunicationIngester(tweetsAsComms, rc);
        
        StopWatch sw = new StopWatch();
        sw.start();
        
        ci.bulkIngest();
        
        sw.stop();
        double time = (double) sw.getTime() / 1000;
        logger.info("Took " + time + " seconds to ingest " + sizeOfComms + " tweets via bulk ingest.");
        
        logger.info("Sleeping for 10s...");
        Thread.sleep(10000);
        rc = (RiakCorpus) rcf.getCorpus(testCorpusName);
        Set<String> commIds = rc.getCommIdSet();
        assertEquals(sizeOfComms, commIds.size());
        
        // delete the corpus
        rcf.deleteCorpus(testCorpusName);
        rcf.close();
    }
}

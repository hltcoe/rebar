package edu.jhu.rebar.riak;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;

import edu.jhu.rebar.RebarBackends;
import edu.jhu.rebar.RebarException;

public class RiakCorpusFactoryTest {

    private static final Logger logger = Logger.getLogger(RiakCorpusFactoryTest.class);
    
    RiakCorpusFactory rcf;
    Bucket bucket;
    IRiakClient irc;
    String corpusName = "unit_test_corpus";
    
    @Before
    public void setUp() throws Exception {
        this.rcf = (RiakCorpusFactory)RebarBackends.RIAK.getCorpusFactory();
        this.irc = RiakFactory.pbcClient();
        this.bucket = this.irc.fetchBucket("corpora").execute();
        
        Set<String> buckets = this.irc.listBuckets();
        for (String s : buckets)
            logger.debug("Got bucket: " + s);
    }

    @After
    public void tearDown() throws Exception {
        this.rcf.deleteCorpus(this.corpusName);
    }

    @Test
    public void testMakeCorpus() throws RebarException {
        // first, test that we can create a corpus that does not exist. 
        RiakCorpus rc = (RiakCorpus)this.rcf.makeCorpus(corpusName);
        assertEquals(rc.getName(), corpusName);
    }

    @Test
    public void testGetCorpus() throws RebarException, InterruptedException {
        String getTest = "get-test";
        this.rcf.deleteCorpus(getTest);
        Thread.sleep(3000);
        this.rcf.makeCorpus(corpusName);
        RiakCorpus trc = (RiakCorpus) this.rcf.makeCorpus(getTest);
        trc.addCommId("1000");
        trc.addCommId("1002");
        trc.addCommId("1001");
        
        trc = this.rcf.storeCorpus(trc);
        
        Thread.sleep(3000);
        
        RiakCorpus c = (RiakCorpus) this.rcf.getCorpus(getTest);
        assertEquals(getTest, c.getName());
        assertEquals(3, c.getCommIdSet().size());
        
        c = (RiakCorpus) this.rcf.getCorpus(corpusName);
        assertEquals(corpusName, c.getName());
        assertEquals(0, c.getCommIdSet().size());
    }

    @Test
    public void testCorpusExists() throws RebarException {
        this.rcf.makeCorpus(this.corpusName);
        assertTrue(this.rcf.corpusExists(corpusName));
        assertFalse(this.rcf.corpusExists("3jt3nt13t"));
    }

    @Test
    public void testListCorpora() throws RebarException {
        String test1 = "junit_test_foo";
        String test2 = "junit_test_bar";
        
        this.rcf.makeCorpus(test1);
        this.rcf.makeCorpus(test2);
        
        Collection<String> corpora = this.rcf.listCorpora();
        assertTrue(corpora.contains(test1));
        assertTrue(corpora.contains(test2));
        assertFalse(corpora.contains("35u23un02g40tg"));
        
        this.rcf.deleteCorpus(test1);
        this.rcf.deleteCorpus(test2);
    }

    @Test
    public void testDeleteCorpus() throws RebarException {
        // test we can not throw up when the corpus doesn't exist.
        this.rcf.deleteCorpus("lh3g3bntg2kbt3n236t");
        
        // create a new corpus, and ensure we can delete it.
        this.rcf.makeCorpus(this.corpusName);
        this.rcf.deleteCorpus(this.corpusName);
        
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ie) {
            logger.debug(ie, ie);
        }
        
        assertFalse(this.rcf.listCorpora().contains(this.corpusName));
    }
    
    @Test
    public void testStoreCorpus() throws RebarException {
        // make a corpus.
        RiakCorpus rc = (RiakCorpus) this.rcf.makeCorpus(corpusName);
        
        // add some comms.
        rc.addCommId("test_id");
        rc.addCommId("test_id3");
        
        // store the corpus. 
        rc = this.rcf.storeCorpus(rc);
        
        // check to see our comm id set is right.
        Set<String> commIdSet = rc.getCommIdSet();
        assertEquals(2, commIdSet.size());
        assertTrue(commIdSet.contains("test_id"));
        assertTrue(commIdSet.contains("test_id3"));
        
        // ensure that when we get it, it's correct.
        rc = (RiakCorpus) this.rcf.getCorpus(corpusName);
        commIdSet = rc.getCommIdSet();
        assertEquals(2, commIdSet.size());
        assertTrue(commIdSet.contains("test_id"));
        assertTrue(commIdSet.contains("test_id3"));
    }
}

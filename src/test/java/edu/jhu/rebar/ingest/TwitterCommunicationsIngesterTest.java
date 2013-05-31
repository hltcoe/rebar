/**
 * 
 */
package edu.jhu.rebar.ingest;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.NewInitializer;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.config.RebarConfiguration;
import edu.jhu.rebar.file.FileCorpusFactory;

/**
 * @author max
 *
 */
public class TwitterCommunicationsIngesterTest {
    
    private FileCorpusFactory fbc;
    private final String corpusName = "twitter_test_corpus";
    
    private static final Logger logger = LoggerFactory
            .getLogger(TwitterCommunicationsIngesterTest.class);
    
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        this.fbc = new FileCorpusFactory(RebarConfiguration
                .getTestFileCorpusDirectory());
        if (this.fbc.corpusExists(this.corpusName))
            this.fbc.deleteCorpus(this.corpusName);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        if (this.fbc.corpusExists(this.corpusName))
            this.fbc.deleteCorpus(this.corpusName);
    }

    @Test
    public void testCompleteIngest() throws RebarException {
        logger.info("Creating initializer: " + corpusName);
        NewInitializer init = this.fbc.createCorpusInitializer(corpusName);
        
        logger.info("Ingesting twitter files...");
        TwitterCommunicationsIngester ingester = 
                new TwitterCommunicationsIngester(init);

        ingester.ingestFile(new File("src/test/resources/100-tweets.txt"));
        
        ingester.finishIngest();
        logger.info("Tweets ingested: " + ingester.getTweetsAdded());
        init.close();
        
        Corpus fc = this.fbc.getCorpus(corpusName);
        assertEquals(ingester.getTweetsAdded(), fc.getNumCommunications());
    }
}

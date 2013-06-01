/**
 * 
 */
package edu.jhu.rebar.examples;

import java.io.File;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.rebar.NewInitializer;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.config.RebarConfiguration;
import edu.jhu.rebar.file.FileCorpusFactory;
import edu.jhu.rebar.ingest.TwitterCommunicationsIngester;

/**
 * @author max
 *
 */
public class ListCorporaTest {

    private ListCorpora lc;
    private final String corpusNameOne = "foo";
    private final String corpusNameTwo = "qux";
    
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        FileCorpusFactory fcf = 
                new FileCorpusFactory(RebarConfiguration.getTestFileCorpusDirectory());
        Set<String> cnSet = fcf.listCorpora();
        for (String cn : cnSet)
            fcf.deleteCorpus(cn);
        
        
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        FileCorpusFactory fcf = 
                new FileCorpusFactory(RebarConfiguration.getTestFileCorpusDirectory());
        Set<String> cnSet = fcf.listCorpora();
        for (String cn : cnSet)
            fcf.deleteCorpus(cn);
    }

    private void initCorporaOne() throws RebarException {
        FileCorpusFactory fcf = 
                new FileCorpusFactory(RebarConfiguration.getTestFileCorpusDirectory());
        NewInitializer init = fcf.createCorpusInitializer(this.corpusNameOne);
        TwitterCommunicationsIngester ingester = 
                new TwitterCommunicationsIngester(init);

        ingester.ingestFile(new File("src/test/resources/100-tweets.txt"));
        
        ingester.finishIngest();
        init.close();
    }
    
    private void initCorporaTwo() throws RebarException {
        FileCorpusFactory fcf = 
                new FileCorpusFactory(RebarConfiguration.getTestFileCorpusDirectory());
        NewInitializer init = fcf.createCorpusInitializer(this.corpusNameTwo);
        TwitterCommunicationsIngester ingester = 
                new TwitterCommunicationsIngester(init);

        ingester.ingestFile(new File("src/test/resources/100-tweets.txt"));
        
        ingester.finishIngest();
        init.close();
    }
    
    /**
     * Test method for {@link edu.jhu.rebar.examples.ListCorpora#list()}.
     */
    @Test
    public void testList() throws RebarException {
        this.initCorporaOne();
        this.initCorporaTwo();
        
        this.lc = new ListCorpora(RebarConfiguration.getTestFileCorpusDirectory());
        this.lc.list();
    }

}

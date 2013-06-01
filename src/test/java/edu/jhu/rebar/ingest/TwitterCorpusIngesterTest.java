/**
 * 
 */
package edu.jhu.rebar.ingest;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.Concrete.Tokenization;
import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.IndexedCommunication;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;
import edu.jhu.rebar.config.RebarConfiguration;
import edu.jhu.rebar.file.FileCorpusFactory;
import edu.jhu.rebar.file.FileCorpusFactory.FileCorpusFactoryInitializer;

/**
 * @author max
 *
 */
public class TwitterCorpusIngesterTest {

    private FileCorpusFactory fbc;
    private final String corpusName = "twitter_test_corpus";
    
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

    private void addTokenizationStage() throws RebarException {
        Corpus c = this.fbc.getCorpus(this.corpusName);
        TwitterCorpusIngester ci = new TwitterCorpusIngester(c);
        ci.addTokenizationStage();
        ci.close();
        c.close();
    }
    
    private void ingestComms() throws RebarException, 
            FileNotFoundException, IOException {
        FileCorpusFactoryInitializer fcfi = this.fbc
                .createCorpusInitializer(this.corpusName);
        TwitterCommunicationsIngester ingester = 
                new TwitterCommunicationsIngester(fcfi);
        ingester.ingestFile(new File("src/test/resources/10-tweets.txt"));
        ingester.finishIngest();
        fcfi.close();
    }
    
    @Test
    public void testAddTokenization() throws RebarException, 
            FileNotFoundException, IOException {
        this.ingestComms();
        this.addTokenizationStage();
        
        Corpus newC = this.fbc.getCorpus(this.corpusName);
        Stage tokStage = newC
                .getStage(TwitterCorpusIngester.TOKENIZATION_STAGE_NAME, 
                TwitterCorpusIngester.TOKENIZATION_STAGE_VERSION);
        assertEquals(0, tokStage.getDependencies().size());
        Corpus.Reader tr = newC.makeReader(tokStage);
        Iterator<IndexedCommunication> icIter = tr.loadCommunications();
        while(icIter.hasNext()) {
            IndexedCommunication ic = icIter.next();
            Communication comm = ic.getProto();
            Tokenization t = comm.getSectionSegmentation(0)
                    .getSection(0)
                    .getSentenceSegmentation(0)
                    .getSentence(0)
                    .getTokenization(0);
            assertFalse(t == null);
            assertTrue(t.getTokenList().size() > 0);
        }
        
        tr.close();
    }
    
//    private void addGraphStage() throws RebarException, FileNotFoundException,
//            IOException {
//        Corpus c = this.fbc.getCorpus(this.corpusName);
//        TwitterCorpusIngester ci = new TwitterCorpusIngester(c);
//        ci.addGraphStage();
//        ci.close();
//        c.close();
//    }
    
//    @Test
//    public void testAddGraph() throws RebarException, 
//            FileNotFoundException, IOException {
//        this.ingestComms();
//        this.addGraphStage();
//        
//        Corpus newC = this.fbc.getCorpus(this.corpusName);
//        Stage graphStage = newC
//                .getStage(TwitterCorpusIngester.INITIAL_GRAPH_STAGE_NAME, 
//                TwitterCorpusIngester.INITIAL_GRAPH_STAGE_VERSION);
//        assertEquals(0, graphStage.getDependencies().size());
//        Corpus.Reader cr = newC.makeReader(graphStage);
//        Iterator<IndexedCommunication> icIter = cr.loadCommunications();
//        while(icIter.hasNext()) {
//            IndexedCommunication ic = icIter.next();
//            IndexedKnowledgeGraph kg = ic.getKnowledgeGraph();
//            assertFalse(kg == null);
//            assertFalse(kg.getUUID() == null);
//        }
//        
//        cr.close();
//    }
    
//
//    @Test
//    public void testAddTokenizationGraphStages() throws RebarException, 
//            FileNotFoundException, IOException {
//        this.ingestComms();
//        this.addTokenizationStage();
//        this.addGraphStage();
//        
//        Corpus newC = this.fbc.getCorpus(this.corpusName);
//        Stage graphStage = newC
//                .getStage(TwitterCorpusIngester.INITIAL_GRAPH_STAGE_NAME, 
//                TwitterCorpusIngester.INITIAL_GRAPH_STAGE_VERSION);
//        Corpus.Reader cr = newC.makeReader(graphStage);
//        Iterator<IndexedCommunication> icIter = cr.loadCommunications();
//        while(icIter.hasNext()) {
//            IndexedCommunication ic = icIter.next();
//            IndexedKnowledgeGraph kg = ic.getKnowledgeGraph();
//            assertFalse(kg == null);
//            assertFalse(kg.getUUID() == null);
//        }
//        
//        cr.close();
//        
//        Stage tokStage = newC
//                .getStage(TwitterCorpusIngester.TOKENIZATION_STAGE_NAME, 
//                TwitterCorpusIngester.TOKENIZATION_STAGE_VERSION);
//        Corpus.Reader tr = newC.makeReader(tokStage);
//        icIter = tr.loadCommunications();
//        while(icIter.hasNext()) {
//            IndexedCommunication ic = icIter.next();
//            Communication comm = ic.getProto();
//            Tokenization t = comm.getSectionSegmentation(0)
//                    .getSection(0)
//                    .getSentenceSegmentation(0)
//                    .getSentence(0)
//                    .getTokenization(0);
//            assertFalse(t == null);
//            assertTrue(t.getTokenList().size() > 0);
//        }
//        
//        tr.close();
//    }
}

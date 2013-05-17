package edu.jhu.rebar.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.RebarException;

public class FileCorpusFactoryTest {

    FileCorpusFactory fcf;
    final String pathString = "target/file_corpus_factory_test";
    
    @Before
    public void setUp() throws Exception {
        fcf = new FileCorpusFactory(pathString);
    }

    @After
    public void tearDown() throws Exception {
        Set<String> corporaList = fcf.listCorpora();
        for (String c : corporaList)
            fcf.deleteCorpus(c);
    }

    @Test
    public void testMakeCorpus() throws RebarException {
        String corpusName = "foo";
        fcf.makeCorpus(corpusName);
        assertTrue(new File(pathString + File.separator + corpusName).exists());
    }
    
    @Test
    public void testMakeCorpusAlreadyExists() throws RebarException {
        String corpusName = "foo";
        fcf.makeCorpus(corpusName);
        fcf.makeCorpus(corpusName);
        assertTrue(new File(pathString + File.separator + corpusName).exists());
    }

    @Test
    public void testGetCorpus() throws RebarException {
        String corpusName = "qux";
        Corpus c = fcf.getCorpus(corpusName);
        assertEquals(corpusName, c.getName());
    }

    @Test
    public void testCorpusExists() throws RebarException {
        String corpusName = "baz";
        fcf.makeCorpus(corpusName);
        assertTrue(fcf.corpusExists(corpusName));
    }

    @Test
    public void testListCorpora() throws RebarException {
        String[] names = new String[] {
            "foo", "bar", "qux"
        };
        
        for (String name : names)
            fcf.makeCorpus(name);
        
        
        Set<String> corporaList = fcf.listCorpora();
        for (String corp : corporaList)
            assertTrue(corporaList.contains(corp));
    }

    @Test
    public void testDeleteCorpus() throws RebarException {
        String corpusName = "noo";
        fcf.makeCorpus(corpusName);
        fcf.deleteCorpus(corpusName);
        fcf.deleteCorpus(corpusName);
        fcf.deleteCorpus(corpusName);
    }

}

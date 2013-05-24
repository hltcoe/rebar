package edu.jhu.rebar.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.Concrete.CommunicationGUID;
import edu.jhu.rebar.RebarException;

import static org.mockito.Mockito.*;

public class FileBackedCorpusTest {

    FileBackedCorpus fbc;
    final String pathString = "target/file_corpus_test/";
    
    @Before
    public void setUp() throws Exception {
//        this.fbc = new FileBackedCorpus(Paths.get(pathString));
    }

    @After
    public void tearDown() throws Exception {
        this.fbc.close();
        new FileCorpusFactory("target/").deleteCorpus("file_corpus_test");
    }
    
    @Test
    public void initializeCorpusIterator() throws RebarException {
    	
    }
    
    
}

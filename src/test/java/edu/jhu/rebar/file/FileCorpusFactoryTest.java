package edu.jhu.rebar.file;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.Concrete.CommunicationGUID;
import edu.jhu.hlt.concrete.Concrete.KnowledgeGraph;
import edu.jhu.hlt.concrete.util.ProtoFactory;
import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.util.FileUtil;

public class FileCorpusFactoryTest {

    FileCorpusFactory fcf;
    final String pathString = "target/file_corpus_factory_test";
    
    ProtoFactory pf = new ProtoFactory();
	CommunicationGUID guidOne = pf.generateMockCommGuid();
	KnowledgeGraph kg = pf.generateMockKnowledgeGraph();
	Communication commOne = ProtoFactory.generateCommunication(guidOne, kg);
	
	CommunicationGUID guidTwo = pf.generateMockCommGuid();
	KnowledgeGraph kgTwo = pf.generateMockKnowledgeGraph();
	Communication commTwo = ProtoFactory.generateCommunication(guidTwo, kgTwo);
    
    @Before
    public void setUp() throws Exception {
        fcf = new FileCorpusFactory(Paths.get(pathString));
    }

    @After
    public void tearDown() throws Exception {
    	FileUtil.deleteFolderAndSubfolders(Paths.get(pathString));
    }
    
    @Test
    public void testNoCorporaAfterInit() throws RebarException {
    	assertEquals(0, this.fcf.corporaList.size());
    }
    
    @Test
    public void testNonExistentCorpus() throws RebarException {
    	assertFalse(this.fcf.corpusExists("bar"));
    }
    
    @Test
    public void testInitializeCorpus() throws RebarException {
    	Iterator<Communication> commIter = this.generateCommIter();
    	
    	FileBackedCorpus fbc = this.fcf.initializeCorpus("bar", commIter);
    	assertTrue(fbc.getCommIdSet().contains(guidOne.getCommunicationId()));
    	assertTrue(fbc.getCommIdSet().contains(guidTwo.getCommunicationId()));
    }
    
    private Iterator<Communication> generateCommIter() {
    	Iterator<Communication> commIter = mock(Iterator.class);
    	when(commIter.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
    	when(commIter.next())
    		.thenReturn(commOne)
    		.thenReturn(commTwo)
    		.thenThrow(new IllegalArgumentException());
    	return commIter;
    }
    
    @Test
    public void testGetCorpusExists() throws RebarException {
    	Iterator<Communication> commIter = this.generateCommIter();
    	FileBackedCorpus fbc = this.fcf.initializeCorpus("bar", commIter);
    	
    	FileBackedCorpus retCorpus = (FileBackedCorpus) this.fcf.getCorpus("bar");
    	assertTrue(retCorpus.getCommIdSet().contains(guidOne.getCommunicationId()));
    	assertTrue(retCorpus.getCommIdSet().contains(guidTwo.getCommunicationId()));
    	
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

package edu.jhu.rebar.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.Concrete.CommunicationGUID;
import edu.jhu.hlt.concrete.Concrete.KnowledgeGraph;
import edu.jhu.hlt.concrete.util.ProtoFactory;
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
    	assertTrue(this.fcf.corpusExists("bar"));
    	assertTrue(fbc.getCommIdSet().contains(guidOne.getCommunicationId()));
    	assertTrue(fbc.getCommIdSet().contains(guidTwo.getCommunicationId()));
    	
    	Path commPath = Paths.get(pathString)
    			.resolve("bar")
    			.resolve("communications");
    	assertTrue(Files.exists(commPath.resolve(guidOne.getCommunicationId() + ".pb")));
    	assertTrue(Files.exists(commPath.resolve(guidTwo.getCommunicationId() + ".pb")));
    }
    
    @Test(expected = RebarException.class) 
    public void testInitializeCorpusThatExists() throws RebarException {
    	Iterator<Communication> commIter = this.generateCommIter();
    	
    	this.fcf.initializeCorpus("bar", commIter);
    	this.fcf.initializeCorpus("bar", commIter);
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
    	this.fcf.initializeCorpus("bar", commIter);
    	
    	FileBackedCorpus retCorpus = (FileBackedCorpus) this.fcf.getCorpus("bar");
    	assertTrue(retCorpus.getCommIdSet().contains(guidOne.getCommunicationId()));
    	assertTrue(retCorpus.getCommIdSet().contains(guidTwo.getCommunicationId()));
    }
    
    @Test(expected = RebarException.class) 
    public void testGetCorpusNotExists() throws RebarException {
    	this.fcf.getCorpus("bar");
    }
    
    @Test
    public void testGetCorpusSizeUpdates() throws RebarException {
    	Iterator<Communication> commIter = this.generateCommIter();
    	this.fcf.initializeCorpus("bar", commIter);
    	
    	assertEquals(1, this.fcf.listCorpora().size());
    }

    @Test(expected = RebarException.class) 
    public void testDeleteCorpusNotExists() throws RebarException {
    	this.fcf.deleteCorpus("bar");
    }
    
    @Test
    public void testDeleteCorpus() throws RebarException {
    	Iterator<Communication> commIter = this.generateCommIter();
    	this.fcf.initializeCorpus("bar", commIter);
    	
    	this.fcf.deleteCorpus("bar");
    	assertFalse(this.fcf.corpusExists("bar"));
    }
}

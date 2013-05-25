package edu.jhu.rebar.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.Concrete.CommunicationGUID;
import edu.jhu.hlt.concrete.Concrete.KnowledgeGraph;
import edu.jhu.hlt.concrete.util.ProtoFactory;
import edu.jhu.rebar.RebarException;

import static org.mockito.Mockito.*;

public class FileBackedCorpusTest {

	final String pathString = "target/file_backed_corpus_test/";
	final String corpusName = "testCorpus";

	ProtoFactory pf = new ProtoFactory();
	CommunicationGUID guidOne = pf.generateMockCommGuid();
	KnowledgeGraph kg = pf.generateMockKnowledgeGraph();
	Communication commOne = ProtoFactory.generateCommunication(guidOne, kg);
	
	CommunicationGUID guidTwo = pf.generateMockCommGuid();
	KnowledgeGraph kgTwo = pf.generateMockKnowledgeGraph();
	Communication commTwo = ProtoFactory.generateCommunication(guidTwo, kgTwo);
	
	FileBackedCorpus fbc;    
    FileCorpusFactory fcf;
    
    
    @Before
    public void setUp() throws Exception {
    	this.fcf = new FileCorpusFactory(Paths.get(pathString));
    	
    	List<Communication> commList = new ArrayList<>(2);
    	commList.add(commOne);
    	commList.add(commTwo);
    	
    	this.fbc = this.fcf.initializeCorpus(corpusName,
    			commList.iterator());
    }

    @After
    public void tearDown() throws Exception {
        this.fbc.close();
        this.fcf.deleteCorpus(corpusName);
    }
    
    @Test
    public void testGetNumCommunications() throws RebarException {
    	assertEquals(2, this.fbc.getNumCommunications());
    }
}

package edu.jhu.rebar.file;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.Concrete.CommunicationGUID;
import edu.jhu.hlt.concrete.Concrete.KnowledgeGraph;
import edu.jhu.hlt.concrete.util.ProtoFactory;
import edu.jhu.rebar.Corpus.Reader;
import edu.jhu.rebar.IndexedCommunication;
import edu.jhu.rebar.RebarException;

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
    
    @Test
    public void testLoadCommunicationString() throws RebarException {
    	Reader r = this.fbc.makeReader();
    	String guidOneString = guidOne.getCommunicationId();
    	IndexedCommunication ic = 
    			r.loadCommunication(guidOneString);
    	assertEquals(commOne.getUuid(), ic.getProto().getUuid());
    	assertEquals(guidOneString, ic.getCommunicationId());
    	assertEquals("testCorpus", ic.getCorpusName());
    }
    
    @Test(expected = RebarException.class)
    public void testLoadCommunicationStringNotExist() throws RebarException {
    	Reader r = this.fbc.makeReader();
    	String guidOneString = guidOne.getCommunicationId();
    	r.loadCommunication(guidOneString + "50953-135ff");
    }
    
    @Test(expected = RebarException.class)
    public void testLoadCommunicationIteratorNotExist() throws RebarException {
    	Reader r = this.fbc.makeReader();
    	String invalidCommId = "foobarqux351351";
    	Collection<String> iColl = new ArrayList<>(1);
    	iColl.add(invalidCommId);
    	r.loadCommunications(iColl);
    }
    
    @Test
    public void testLoadCommunicationIteratorSubset() throws RebarException {
    	Reader r = this.fbc.makeReader();
    	Collection<String> iColl = new ArrayList<>(1);
    	iColl.add(guidOne.getCommunicationId());
    	Iterator<IndexedCommunication> icIter = r.loadCommunications(iColl);
    	IndexedCommunication ic = icIter.next();
    	
    	assertEquals(guidOne.getCommunicationId(), ic.getCommunicationId());
    	assertFalse(icIter.hasNext());
    }
    
    @Test
    public void testLoadCommunicationIteratorAll() throws RebarException {
    	Reader r = this.fbc.makeReader();
    	Iterator<IndexedCommunication> icIter = r.loadCommunications();
    	List<IndexedCommunication> commList = new ArrayList<>();
    	while (icIter.hasNext())
    		commList.add(icIter.next());
    	
    	assertEquals(2, commList.size());
    }
}

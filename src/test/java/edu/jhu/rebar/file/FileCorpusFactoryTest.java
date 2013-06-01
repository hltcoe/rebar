package edu.jhu.rebar.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.Concrete.CommunicationGUID;
import edu.jhu.hlt.concrete.Concrete.KnowledgeGraph;
import edu.jhu.hlt.concrete.ConcreteException;
import edu.jhu.hlt.concrete.io.ProtocolBufferReader;
import edu.jhu.hlt.concrete.util.ProtoFactory;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.config.RebarConfiguration;
import edu.jhu.rebar.util.FileUtil;

public class FileCorpusFactoryTest {

    FileCorpusFactory fcf;

    ProtoFactory pf = new ProtoFactory();
    CommunicationGUID guidOne = pf.generateMockCommGuid();
    KnowledgeGraph kg = pf.generateMockKnowledgeGraph();
    Communication commOne = ProtoFactory.generateCommunication(guidOne, kg);

    CommunicationGUID guidTwo = pf.generateMockCommGuid();
    KnowledgeGraph kgTwo = pf.generateMockKnowledgeGraph();
    Communication commTwo = ProtoFactory.generateCommunication(guidTwo, kgTwo);

    @Before
    public void setUp() throws Exception {
        fcf = new FileCorpusFactory(RebarConfiguration
                .getTestFileCorpusDirectory());
    }

    @After
    public void tearDown() throws Exception {
        FileUtil.deleteFolderAndSubfolders(RebarConfiguration
                .getTestFileCorpusDirectory());
    }

    @Test
    public void testNoCorporaAfterInit() throws RebarException {
        assertEquals(0, this.fcf.listCorpora().size());
    }

    @Test
    public void testNonExistentCorpus() throws RebarException {
        assertFalse(this.fcf.corpusExists("bar"));
    }

    @Test
    public void testInitializeCorpus() throws RebarException,
            ConcreteException, IOException {
        Iterator<Communication> commIter = this
                .generateMockCommunicationIterator();

        FileBackedCorpus fbc = this.fcf.initializeAndSealCorpus("bar", commIter);
        assertTrue(this.fcf.corpusExists("bar"));
        assertTrue(fbc.getCommIdSet().contains(guidOne.getCommunicationId()));
        assertTrue(fbc.getCommIdSet().contains(guidTwo.getCommunicationId()));

        Path commPath = RebarConfiguration.getTestFileCorpusDirectory()
                .resolve("bar")
                .resolve("communications");

        assertTrue(Files.exists(commPath.resolve(guidOne.getCommunicationId()
                + ".pb")));
        assertTrue(Files.exists(commPath.resolve(guidTwo.getCommunicationId()
                + ".pb")));

        File iCommOne = commPath.resolve(guidOne.getCommunicationId() + ".pb")
                .toFile();
        ProtocolBufferReader pbr = new ProtocolBufferReader(
                new FileInputStream(iCommOne), Communication.class);
        Communication icOne = (Communication) pbr.next();
        assertEquals(commOne.getUuid(), icOne.getUuid());
        assertEquals("bar", icOne.getGuid().getCorpusName());
        pbr.close();

        File iCommTwo = commPath.resolve(guidTwo.getCommunicationId() + ".pb")
                .toFile();
        pbr = new ProtocolBufferReader(new FileInputStream(iCommTwo),
                Communication.class);
        Communication icTwo = (Communication) pbr.next();
        assertEquals(commTwo.getUuid(), icTwo.getUuid());
        assertEquals("bar", icTwo.getGuid().getCorpusName());
        pbr.close();
    }
    
    @Test
    public void testInitializeCorpusCollection() throws RebarException,
            ConcreteException, IOException {
        Collection<Communication> commColl = 
                this.generateMockCommunicationCollection();

        FileBackedCorpus fbc = this.fcf.initializeAndSealCorpus("bar", commColl);
        assertTrue(this.fcf.corpusExists("bar"));
        assertTrue(fbc.getCommIdSet().contains(guidOne.getCommunicationId()));
        assertTrue(fbc.getCommIdSet().contains(guidTwo.getCommunicationId()));

        Path commPath = RebarConfiguration.getTestFileCorpusDirectory()
                .resolve("bar")
                .resolve("communications");

        assertTrue(Files.exists(commPath.resolve(guidOne.getCommunicationId()
                + ".pb")));
        assertTrue(Files.exists(commPath.resolve(guidTwo.getCommunicationId()
                + ".pb")));

        File iCommOne = commPath.resolve(guidOne.getCommunicationId() + ".pb")
                .toFile();
        ProtocolBufferReader pbr = new ProtocolBufferReader(
                new FileInputStream(iCommOne), Communication.class);
        Communication icOne = (Communication) pbr.next();
        assertEquals(commOne.getUuid(), icOne.getUuid());
        assertEquals("bar", icOne.getGuid().getCorpusName());
        pbr.close();

        File iCommTwo = commPath.resolve(guidTwo.getCommunicationId() + ".pb")
                .toFile();
        pbr = new ProtocolBufferReader(new FileInputStream(iCommTwo),
                Communication.class);
        Communication icTwo = (Communication) pbr.next();
        assertEquals(commTwo.getUuid(), icTwo.getUuid());
        assertEquals("bar", icTwo.getGuid().getCorpusName());
        pbr.close();
    }

    @Test(expected = RebarException.class)
    public void testInitializeCorpusThatExists() throws RebarException {
        Iterator<Communication> commIter = this
                .generateMockCommunicationIterator();

        this.fcf.initializeAndSealCorpus("bar", commIter);
        this.fcf.initializeAndSealCorpus("bar", commIter);
    }

    public Iterator<Communication> generateMockCommunicationIterator() {
        Iterator<Communication> commIter = mock(Iterator.class);
        when(commIter.hasNext()).thenReturn(true).thenReturn(true)
                .thenReturn(false);
        when(commIter.next()).thenReturn(commOne).thenReturn(commTwo)
                .thenThrow(new IllegalArgumentException());
        return commIter;
    }
    
    public Collection<Communication> generateMockCommunicationCollection() {
        Collection<Communication> commColl = new ArrayList<>(2);
        commColl.add(commOne);
        commColl.add(commTwo);
        return commColl;
    }

    @Test
    public void testGetCorpusExists() throws RebarException {
        Iterator<Communication> commIter = this
                .generateMockCommunicationIterator();
        this.fcf.initializeAndSealCorpus("bar", commIter);

        FileBackedCorpus retCorpus = (FileBackedCorpus) this.fcf
                .getCorpus("bar");
        assertTrue(retCorpus.getCommIdSet().contains(
                guidOne.getCommunicationId()));
        assertTrue(retCorpus.getCommIdSet().contains(
                guidTwo.getCommunicationId()));
    }

    @Test(expected = RebarException.class)
    public void testGetCorpusNotExists() throws RebarException {
        this.fcf.getCorpus("bar");
    }

    @Test
    public void testGetCorpusSizeUpdates() throws RebarException {
        Iterator<Communication> commIter = this
                .generateMockCommunicationIterator();
        this.fcf.initializeAndSealCorpus("bar", commIter);

        assertEquals(1, this.fcf.listCorpora().size());
    }

    @Test(expected = RebarException.class)
    public void testDeleteCorpusNotExists() throws RebarException {
        this.fcf.deleteCorpus("bar");
    }

    @Test
    public void testDeleteCorpus() throws RebarException {
        Iterator<Communication> commIter = this
                .generateMockCommunicationIterator();
        this.fcf.initializeAndSealCorpus("bar", commIter);

        this.fcf.deleteCorpus("bar");
        assertFalse(this.fcf.corpusExists("bar"));
    }
}

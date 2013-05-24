package edu.jhu.rebar.file;

import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.Concrete.CommunicationGUID;
import edu.jhu.hlt.concrete.Concrete.KnowledgeGraph;
import edu.jhu.hlt.concrete.ConcreteException;
import edu.jhu.hlt.concrete.io.ProtocolBufferReader;
import edu.jhu.hlt.concrete.util.ProtoFactory;
import edu.jhu.rebar.IndexedCommunication;
import edu.jhu.rebar.ProtoIndex;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;

public class FileCorpusWriterTest {

    private FileBackedCorpus fbc;
    private FileStage stage;
    
    private final String pathString = "target/file_corpus_test/";
    
    @Before
    public void setUp() throws Exception {
        this.fbc = new FileBackedCorpus(Paths.get(pathString));
        this.stage = (FileStage)this.fbc.makeStage("ingest", "v1.0", new TreeSet<Stage>(), "Ingest test stage", false);
    }

    @After
    public void tearDown() throws Exception {
        this.fbc.close();
        new FileCorpusFactory("target/").deleteCorpus("file_corpus_test");
    }

    @Test
    public void testSaveCommunication() throws RebarException, FileNotFoundException, ConcreteException, IOException {
        String corpusName = "file_corpus_test";
        String commId = "file_corpus_test_9351035";
        
        CommunicationGUID guid = ProtoFactory.generateCommGuid(corpusName, commId);
        KnowledgeGraph kb = ProtoFactory.generateKnowledgeGraph();
        Communication comm = ProtoFactory.generateCommunication(guid, kb);
        
        FileCorpusWriter fcw = new FileCorpusWriter(this.stage);
        ProtoIndex pi = new ProtoIndex(comm);
        fcw.saveCommunication(new IndexedCommunication(comm, pi, null));
        
        ProtocolBufferReader pbr = new ProtocolBufferReader(
                new FileInputStream(
                        this.stage.getPath().resolve(commId).toString()), Communication.class);
        Communication readComm = (Communication) pbr.next();
        assertEquals(comm.getUuid(), readComm.getUuid());
        pbr.close();
    }
}

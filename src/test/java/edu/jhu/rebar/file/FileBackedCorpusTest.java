package edu.jhu.rebar.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.concrete.Concrete.Communication;
import edu.jhu.concrete.ConcreteException;
import edu.jhu.concrete.io.ProtocolBufferReader;
import edu.jhu.concrete.util.ProtoFactory;
import edu.jhu.rebar.Corpus.Writer;
import edu.jhu.rebar.IndexedCommunication;
import edu.jhu.rebar.ProtoIndex;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;

public class FileBackedCorpusTest {

    FileBackedCorpus fbc;
    final String pathString = "target/file_corpus_test/";
    
    @Before
    public void setUp() throws Exception {
        this.fbc = new FileBackedCorpus(Paths.get(pathString));
    }

    @After
    public void tearDown() throws Exception {
        this.fbc.close();
        new FileCorpusFactory("target/").deleteCorpus("file_corpus_test");
    }

    @Test
    public void testFileBackedCorpus() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetPath() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetName() throws RebarException {
        assertEquals("file_corpus_test", this.fbc.getName());
    }

    @Test
    public void testClose() {
        fail("Not yet implemented");
    }

    @Test
    public void testMakeStageNotExists() throws RebarException {
        this.fbc.makeStage("test_stage", "ingest", new TreeSet<Stage>(), "Unit test stage", false);
    }
    
    @Test(expected = RebarException.class)
    public void testMakeStageAlreadyExists() throws RebarException {
        this.fbc.makeStage("test_stage", "ingest", new TreeSet<Stage>(), "Unit test stage", false);
        this.fbc.makeStage("test_stage", "ingest", new TreeSet<Stage>(), "Unit test stage", false);
    }

    @Test
    public void testMarkStagePublic() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetStageStringString() throws RebarException {
        this.fbc.makeStage("test_stage", "ingest", new TreeSet<Stage>(), "Unit test stage", false);
        Stage s = this.fbc.getStage("test_stage", "ingest");
        assertEquals(1, s.getStageId());
        assertEquals("test_stage", s.getStageName());
        assertEquals("ingest", s.getStageVersion());
        assertTrue(s.getDependencies().size() == 0);
        assertEquals("Unit test stage", s.getDescription());
    }
    
    @Test(expected = RebarException.class)
    public void testGetStageNotExist() throws RebarException {
        this.fbc.getStage(15135);
    }

    @Test
    public void testGetStageInt() throws RebarException {
        this.fbc.makeStage("test_stage", "ingest", new TreeSet<Stage>(), "Unit test stage", false);
        Stage s = this.fbc.getStage(1);
        assertEquals(1, s.getStageId());
        assertEquals("test_stage", s.getStageName());
        assertEquals("ingest", s.getStageVersion());
        assertTrue(s.getDependencies().size() == 0);
        assertEquals("Unit test stage", s.getDescription());
    }

    @Test
    public void testGetStageString() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetStages() {
        fail("Not yet implemented");
    }

    @Test
    public void testHasStageNoStage() throws RebarException {
        assertFalse(this.fbc.hasStage("foobar", "v1.0.0"));
    }
    
    @Test
    public void testHasStageStageExists() throws RebarException {
        this.fbc.makeStage("test_stage", "ingest", new TreeSet<Stage>(), "Unit test stage", true);
        assertTrue(this.fbc.hasStage("test_stage", "ingest"));
    }

    @Test
    public void testDeleteStage() {
        fail("Not yet implemented");
    }

    @Test
    public void testReadComIdSet() {
        fail("Not yet implemented");
    }

    @Test
    public void testRegisterComIdSet() {
        fail("Not yet implemented");
    }

    @Test
    public void testLookupComIdSet() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetComIdSetNames() {
        fail("Not yet implemented");
    }

    @Test
    public void testMakeInitializer() {
        fail("Not yet implemented");
    }

    @Test
    public void testMakeReaderCollectionOfStage() {
        fail("Not yet implemented");
    }

    @Test
    public void testMakeReaderStageArray() {
        fail("Not yet implemented");
    }

    @Test
    public void testMakeReaderStage() {
        fail("Not yet implemented");
    }

    @Test
    public void testMakeReader() {
        fail("Not yet implemented");
    }

    @Test
    public void testMakeReaderCollectionOfStageBoolean() {
        fail("Not yet implemented");
    }

    @Test
    public void testMakeReaderStageArrayBoolean() {
        fail("Not yet implemented");
    }

    @Test
    public void testMakeReaderStageBoolean() {
        fail("Not yet implemented");
    }

    @Test
    public void testMakeReaderBoolean() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetNumCommunications() {
        fail("Not yet implemented");
    }

    @Test
    public void testMakeWriter() throws RebarException, FileNotFoundException, ConcreteException {
        Stage s = this.fbc.makeStage("test_stage", "ingest", new TreeSet<Stage>(), "Unit test stage", false);
        Writer w = this.fbc.makeWriter(s);
        Communication c = new ProtoFactory().generateMockCommunication();
        ProtoIndex pi = new ProtoIndex(c);
        IndexedCommunication ic = new IndexedCommunication(c, pi, null);
        w.saveCommunication(ic);
        
        Path outputPath = Paths.get(this.pathString).resolve("stages").resolve("test_stage").resolve("ingest").resolve(ic.getCommunicationId());
        assertTrue(Files.exists(outputPath));
        ProtocolBufferReader pbr = new ProtocolBufferReader(new FileInputStream(outputPath.toFile()), Communication.class);
        Communication comm = (Communication) pbr.next();
        assertEquals(c.getUuid(), comm.getUuid());
    }

    @Test
    public void testMakeDiffWriter() {
        fail("Not yet implemented");
    }

}

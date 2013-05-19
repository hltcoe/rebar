package edu.jhu.rebar.file;

import static org.junit.Assert.*;

import java.nio.file.Paths;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        this.fbc.makeStage("test_stage", "ingest", null, "Unit test stage", false);
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
    public void testMakeWriter() {
        fail("Not yet implemented");
    }

    @Test
    public void testMakeDiffWriter() {
        fail("Not yet implemented");
    }

}

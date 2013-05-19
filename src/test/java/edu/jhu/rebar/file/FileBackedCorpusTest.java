package edu.jhu.rebar.file;

import static org.junit.Assert.*;

import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.rebar.RebarException;

public class FileBackedCorpusTest {

    FileBackedCorpus fbc;
    final String pathString = "target/file_corpus_factory_test/file_corpus_test";
    
    @Before
    public void setUp() throws Exception {
        this.fbc = new FileBackedCorpus(Paths.get(pathString));
    }

    @After
    public void tearDown() throws Exception {
        
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
    public void testMakeStage() throws RebarException {
        this.fbc.makeStage("test_stage", "ingest", null, "Unit test stage", false);
        this.fbc.makeStage("test_stage", "ingest", null, "Unit test stage", false);
        this.fbc.makeStage("test_stage", "ingest", null, "Unit test stage", false);
    }

    @Test
    public void testMarkStagePublic() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetStageStringString() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetStageInt() {
        fail("Not yet implemented");
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
    public void testHasStage() {
        fail("Not yet implemented");
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

package edu.jhu.rebar.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Descriptors.FieldDescriptor;

import edu.jhu.hlt.concrete.Concrete;
import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.Concrete.CommunicationGUID;
import edu.jhu.hlt.concrete.Concrete.KnowledgeGraph;
import edu.jhu.hlt.concrete.Concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.Concrete.Token;
import edu.jhu.hlt.concrete.util.ProtoFactory;
import edu.jhu.hlt.tift.ConcreteSectionSegmentation;
import edu.jhu.hlt.tift.Tokenizer;
import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.Corpus.Reader;
import edu.jhu.rebar.IndexedTokenization.TokenSequence;
import edu.jhu.rebar.IndexedCommunication;
import edu.jhu.rebar.IndexedSentence;
import edu.jhu.rebar.IndexedTokenization;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;

public class FileBackedCorpusTest {

    private static final Logger logger = LoggerFactory
            .getLogger(FileBackedCorpusTest.class);

    final String pathString = "target/file_backed_corpus_test/";
    final String corpusName = "testCorpus";

    ProtoFactory pf = new ProtoFactory();
    CommunicationGUID guidOne = pf.generateMockCommGuid();
    KnowledgeGraph kg = pf.generateMockKnowledgeGraph();
    Communication commOne = Communication
            .newBuilder(ProtoFactory.generateCommunication(guidOne, kg))
            .setText("Sample test text for testing")
            .build();

    CommunicationGUID guidTwo = pf.generateMockCommGuid();
    KnowledgeGraph kgTwo = pf.generateMockKnowledgeGraph();
    Communication commTwo = Communication
            .newBuilder(ProtoFactory.generateCommunication(guidTwo, kgTwo))
            .setText("This is test text")
            .build();

    FileBackedCorpus fbc;
    FileCorpusFactory fcf;
    
    private Set<Stage> noDependencySet = new TreeSet<>();
    
    private String testStageName = "test_stage";
    private String testStageVersion = "v1";
    private String testStageDesc = "test desc";

    @Before
    public void setUp() throws Exception {
        this.fcf = new FileCorpusFactory(Paths.get(pathString));

        List<Communication> commList = new ArrayList<>(2);
        commList.add(commOne);
        commList.add(commTwo);

        this.fbc = this.fcf.initializeCorpus(corpusName, commList.iterator());
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
        IndexedCommunication ic = r.loadCommunication(guidOneString);
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
    
    @Test
    public void testNoStagesAfterInit() throws RebarException {
        SortedSet<Stage> stages = this.fbc.getStages();
        assertEquals(0, stages.size());
    }
    
    private boolean assertStageComparisons(Stage s) {
        return
                this.testStageName.equals(s.getStageName()) &&
                this.testStageVersion.equals(s.getStageVersion()) &&
                this.noDependencySet.equals(s.getDependencies()) && 
                this.testStageDesc.equals(s.getDescription());
    }
    
    @Test
    public void testAddStage() throws RebarException {
        this.fbc.makeStage(this.testStageName, 
                            this.testStageVersion, 
                            this.noDependencySet, 
                            this.testStageDesc, true);
        
        SortedSet<Stage> stages = this.fbc.getStages();
        assertEquals(1, stages.size());
        
        Stage s = stages.first();
        
        assertTrue(this.assertStageComparisons(s));
    }
    
    @Test
    public void testQueryStageNameVersion() throws RebarException {
        this.fbc.makeStage(this.testStageName, 
                this.testStageVersion, 
                this.noDependencySet, 
                this.testStageDesc, true);
        
        Stage s = this.fbc.getStage(this.testStageName, this.testStageVersion);
        
        assertTrue(this.assertStageComparisons(s));
    }
    
    @Test
    public void testCreateStageAppendTokenization () throws RebarException {
        // Find "section segmentation" protobuf Message class. 
        final FieldDescriptor ssField = Concrete.Communication
                .getDescriptor()
                .findFieldByName("section_segmentation");

        final Stage testStage = this.fbc.makeStage(this.testStageName, 
                this.testStageVersion, 
                this.noDependencySet, 
                this.testStageDesc, true);
        final Corpus.Reader testReader = this.fbc.makeReader(testStage);
        final Corpus.Writer testWriter = this.fbc.makeWriter(testStage);
        final Iterator<IndexedCommunication> readIter = 
                testReader.loadCommunications();
        while (readIter.hasNext()) {
            IndexedCommunication com = readIter.next();
            SectionSegmentation ssToAppend = ConcreteSectionSegmentation
                    .generateSectionSegmentation(Tokenizer.TWITTER, com.getText());
            com.addField(ssField, ssToAppend);
//            Concrete.LanguageIdentification.Builder lidBuilder = Concrete.LanguageIdentification.newBuilder();
//            Concrete.LanguageIdentification.LanguageProb.Builder lp = Concrete.LanguageIdentification.LanguageProb.newBuilder();
//            lp.setProbability(1.0f);
//            lp.setLanguage("eng");
//            lidBuilder.addLanguage(lp);
//            lidBuilder.setUuid(IdUtil.generateUUID());
//            lidBuilder.getMetadataBuilder().setTool("my-lid");
//            com.addField(com.getProto(), lidField, lidBuilder.build());
            testWriter.saveCommunication(com);
        }
        testWriter.close();
        testReader.close();
        
        Stage retStage = this.fbc.getStage(this.testStageName, this.testStageVersion);
        Corpus.Reader reader = this.fbc.makeReader(retStage);
        assertTrue(reader.getInputStages().contains(retStage));
        
        Iterator<IndexedCommunication> iter = reader.loadCommunications();
        while (iter.hasNext()) {
            IndexedCommunication ic = iter.next();
            IndexedSentence is = ic.getSentences().get(0);
            IndexedTokenization it = is.getTokenization();
            TokenSequence ts = it.getBestTokenSequence();
            Iterator<Token> tokenIter = ts.iterator();
            logger.info("Communication GUID: " + guidOne.getCommunicationId() + 
                    " has the following tokens:");
            while (tokenIter.hasNext()) {
                Token tok = tokenIter.next();
                logger.info("Token has id: " + tok.getTokenId() + 
                        " and text: " + tok.getText());
            }
        }
    }
}

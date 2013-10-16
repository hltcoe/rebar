package edu.jhu.hlt.rebar.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

import com.google.protobuf.Descriptors.FieldDescriptor;

import edu.jhu.hlt.concrete.Concrete;
import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.Concrete.CommunicationGUID;

import edu.jhu.hlt.concrete.Concrete.LanguageIdentification;
import edu.jhu.hlt.concrete.Concrete.LanguageIdentification.LanguageProb;
import edu.jhu.hlt.concrete.Concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.Concrete.Token;
import edu.jhu.hlt.concrete.util.IdUtil;
import edu.jhu.hlt.concrete.util.ProtoFactory;
import edu.jhu.hlt.rebar.Corpus;
import edu.jhu.hlt.rebar.IndexedCommunication;
import edu.jhu.hlt.rebar.IndexedSentence;
import edu.jhu.hlt.rebar.IndexedTokenization;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Stage;
import edu.jhu.hlt.rebar.Corpus.Reader;
import edu.jhu.hlt.rebar.IndexedTokenization.TokenSequence;
import edu.jhu.hlt.rebar.config.RebarConfiguration;
import edu.jhu.hlt.rebar.file.FileBackedCorpus;
import edu.jhu.hlt.rebar.file.FileCorpusFactory;
import edu.jhu.hlt.rebar.file.FileStage;
import edu.jhu.hlt.tift.ConcreteSectionSegmentation;
import edu.jhu.hlt.tift.Tokenizer;

public class FileBackedCorpusTest {

    final String corpusName = "testCorpus";

    ProtoFactory pf = new ProtoFactory();
    CommunicationGUID guidOne = pf.generateMockCommGuid();

    Communication commOne = Communication
            .newBuilder(ProtoFactory.generateCommunication(guidOne))
            .setText("Sample test text for testing")
            .build();
    List<String> tokensCommOne = new ArrayList<>();
    List<String> tokensCommTwo = new ArrayList<>();

    CommunicationGUID guidTwo = pf.generateMockCommGuid();
    Communication commTwo = Communication
            .newBuilder(ProtoFactory.generateCommunication(guidTwo))
            .setText("This is test text")
            .build();

    FileBackedCorpus fbc;
    FileCorpusFactory fcf;
    
    private Set<Stage> noDependencySet = new TreeSet<>();
    
    private String testStageName = "test_stage";
    private String testStageVersion = "v1";
    private String testStageDesc = "test desc";
    
    private String tokStageName = "tok_stage";
    private String tokStageVersion = "v1";
    private String tokStageDesc = "Test Tokenization";

    private String lidStageName = "lid_stage";
    private String lidStageVersion = "v1";
    private String lidStageDesc = "Test LID";

    @Before
    public void setUp() throws Exception {
        this.fcf = new FileCorpusFactory(RebarConfiguration
                .getTestFileCorpusDirectory());

        List<Communication> commList = new ArrayList<>(2);
        commList.add(commOne);
        commList.add(commTwo);

        this.fbc = this.fcf.initializeCorpus(corpusName, commList.iterator());
        
        tokensCommOne.add("Sample");
        tokensCommOne.add("test");
        tokensCommOne.add("text");
        tokensCommOne.add("for");
        tokensCommOne.add("testing");
        
        tokensCommTwo.add("This");
        tokensCommTwo.add("is");
        tokensCommTwo.add("test");
        tokensCommTwo.add("text");
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
    
    @Test(expected = RebarException.class)
    public void testLoadCommunicationsStageNotExists() throws RebarException {
        Stage s = new FileStage("foo", "v1", 1, this.fbc.getPath(),
                this.noDependencySet, "test stage", true);
        this.fbc.makeReader(s);
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
    
    private void addTokenization() throws RebarException {
        // Find "section segmentation" protobuf Message class. 
        final FieldDescriptor ssField = Concrete.Communication
                .getDescriptor()
                .findFieldByName("section_segmentation");

        final Stage testStage = this.fbc.makeStage(this.tokStageName, 
                this.tokStageVersion, 
                this.noDependencySet, 
                this.tokStageDesc, true);
        final Corpus.Reader testReader = this.fbc.makeReader(testStage);
        final Corpus.Writer testWriter = this.fbc.makeWriter(testStage);
        final Iterator<IndexedCommunication> readIter = 
                testReader.loadCommunications();
        while (readIter.hasNext()) {
            IndexedCommunication com = readIter.next();
            SectionSegmentation ssToAppend = ConcreteSectionSegmentation
                    .generateSectionSegmentation(Tokenizer.TWITTER, com.getText());
            com.addField(ssField, ssToAppend);
            testWriter.saveCommunication(com);
        }
        
        testWriter.close();
        testReader.close();
    }
    
    private void addLanguageIdDependsTokenization() throws RebarException {
        this.addTokenization();
        
        ArrayList<Stage> dependencies = new ArrayList<Stage>();
        dependencies.add(this.fbc.getStage(this.tokStageName, this.tokStageVersion));
        
        final FieldDescriptor lidField = Concrete.Communication
                .getDescriptor()
                .findFieldByName("language_id");

        final Stage lidStage = this.fbc.makeStage(this.lidStageName, 
                this.lidStageVersion, 
                new TreeSet<>(dependencies),
                this.lidStageDesc, true);
        final Corpus.Reader testReader = this.fbc.makeReader(lidStage);
        final Corpus.Writer testWriter = this.fbc.makeWriter(lidStage);
        final Iterator<IndexedCommunication> readIter = 
                testReader.loadCommunications();
        while (readIter.hasNext()) {
            IndexedCommunication com = readIter.next();
            Concrete.LanguageIdentification.Builder lidBuilder = Concrete.LanguageIdentification.newBuilder();
            Concrete.LanguageIdentification.LanguageProb.Builder lp = Concrete.LanguageIdentification.LanguageProb.newBuilder();
            lp.setProbability(1.0f);
            lp.setLanguage("eng");
            lidBuilder.addLanguage(lp);
            lidBuilder.setUuid(IdUtil.generateUUID());
            lidBuilder.getMetadataBuilder().setTool("my-lid");
            com.addField(com.getProto(), lidField, lidBuilder.build());
            testWriter.saveCommunication(com);
        }
        
        testWriter.close();
        testReader.close();
    }
    
    @Test
    public void testCreateStageAppendTokenization () throws RebarException {
        this.addTokenization();
        
        Stage retStage = this.fbc.getStage(this.tokStageName, this.tokStageVersion);
        Corpus.Reader reader = this.fbc.makeReader(retStage);
        assertTrue(reader.getInputStages().contains(retStage));
        
        Iterator<IndexedCommunication> iter = reader.loadCommunications();
        while (iter.hasNext()) {
            IndexedCommunication ic = iter.next();
            
            IndexedSentence is = ic.getSentences().get(0);
            IndexedTokenization it = is.getTokenization();
            TokenSequence ts = it.getBestTokenSequence();
            Iterator<Token> tokenIter = ts.iterator();
            String commId = ic.getCommunicationId();
            
            while (tokenIter.hasNext()) {
                Token tok = tokenIter.next();
                if (commId.equals(guidOne.getCommunicationId()))
                    assertTrue(this.tokensCommOne.contains(tok.getText()));
                else if (commId.equals(guidTwo.getCommunicationId()))
                    assertTrue(this.tokensCommTwo.contains(tok.getText()));
                else
                    fail("Misplaced tokenization: " + commId + " doesn't exist.");
            }
        }
    }
    
    @Test
    public void testCreateStageAppendTokenizationLid () throws RebarException {
        this.addLanguageIdDependsTokenization();
        
        Stage lidStage = this.fbc.getStage(this.lidStageName, this.lidStageVersion);
        Corpus.Reader reader = this.fbc.makeReader(lidStage);
        assertTrue(reader.getInputStages().contains(lidStage));
        
        Iterator<IndexedCommunication> iter = reader.loadCommunications();
        while (iter.hasNext()) {
            IndexedCommunication ic = iter.next();
            IndexedSentence is = ic.getSentences().get(0);
            IndexedTokenization it = is.getTokenization();
            TokenSequence ts = it.getBestTokenSequence();
            Iterator<Token> tokenIter = ts.iterator();
            String commId = ic.getCommunicationId();
            
            List<LanguageIdentification> lidList = 
                    ic.getProto().getLanguageIdList();
            assertEquals(1, lidList.size());
            LanguageIdentification lid = lidList.get(0);
            assertEquals(1, lid.getLanguageCount());
            LanguageProb lp = lid.getLanguage(0);
            assertEquals("eng", lp.getLanguage());
            
            while (tokenIter.hasNext()) {
                Token tok = tokenIter.next();
                if (commId.equals(guidOne.getCommunicationId())) {
                    assertTrue(this.tokensCommOne.contains(tok.getText()));
                    
                } else if (commId.equals(guidTwo.getCommunicationId())) {
                    assertTrue(this.tokensCommTwo.contains(tok.getText()));
                    
                } else {
                    fail("Misplaced tokenization: " + commId + " doesn't exist.");
                }
            }
        }
    }
}

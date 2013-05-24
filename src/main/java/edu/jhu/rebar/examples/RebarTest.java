/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.rebar.examples;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Descriptors.FieldDescriptor;

import edu.jhu.hlt.concrete.Concrete;
import edu.jhu.hlt.concrete.util.IdUtil;
import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.CorpusFactory;
import edu.jhu.rebar.IndexedCommunication;
import edu.jhu.rebar.RebarBackends;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;
import edu.jhu.rebar.file.FileCorpusFactory;

public class RebarTest {
    
    private static final Logger logger = LoggerFactory.getLogger(RebarTest.class);
    private static final SecureRandom sr = new SecureRandom();

    public static void main(String[] args) throws RebarException {
        String corpusName = "mock_test_corpus";
        CorpusFactory cf = RebarBackends.FILE.getCorpusFactory();
        if (cf.corpusExists(corpusName)) {
            //logger.error("A corpus by the name of: " + corpusName + " already exists. Try to run the program again.");
            //System.exit(-1);
            logger.info("Deleting corpus: " + corpusName);
            new FileCorpusFactory().deleteCorpus(corpusName);
        }
        // AccumuloBackedCorpus.deleteCorpus(corpusName);
        Corpus corpus = cf.makeCorpus(corpusName);
        summarize(corpus);
        initialize(corpus);
        summarize(corpus);
        writeLid(corpus, new TreeSet<Stage>(), "4.0");
        summarize(corpus);

        ArrayList<Stage> stages = new ArrayList<Stage>();
        stages.add(corpus.getStage("lid", "4.0"));
        // stages.add(corpus.getStage("lid", "2.0"));
        read(corpus, stages);
        summarize(corpus);
        corpus.close();
    }

    public static void summarize(Corpus corpus) throws RebarException {
        Set<Stage> stages = corpus.getStages();
        logger.info("================== CORPUS " + corpus.getName() + " ================");
        logger.info("  Stages:");
        for (Stage stage : stages) {
            logger.info("    " + stage.getStageId() + ") " + stage.getStageName() + " [" + stage.getStageVersion() + "]");
        }
//        logger.info("  Subsets:");
//        for (String name : corpus.getComIdSetNames()) {
//            Collection<String> idset = corpus.lookupComIdSet(name);
//            logger.info("    * " + name + " (" + idset.size() + " ids)");
//        }
    }

    public static void writeLid(Corpus corpus, Set<Stage> deps, String version) throws RebarException {
        // Our output field:
        final FieldDescriptor lidField = Concrete.Communication.getDescriptor().findFieldByName("language_id");

        logger.info("Writing...");
        final Stage lidStage = corpus.makeStage("lid", version, deps, "test stage", true);
        final Corpus.Reader reader = corpus.makeReader(deps);
        final Corpus.Writer lidWriter = corpus.makeWriter(lidStage);
        final Iterator<IndexedCommunication> readIter = reader.loadCommunications();
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
            lidWriter.saveCommunication(com);
        }
        reader.close();
        lidWriter.close();
    }

    public static void read(Corpus corpus, Collection<Stage> stages) throws RebarException {
        Collection<String> subset = Arrays.asList(new String[] { "doc-3", "doc-9" });
        corpus.registerComIdSet("my-ids", subset);

        logger.info("Reading...");
        Corpus.Reader reader = corpus.makeReader(stages);
        subset = corpus.lookupComIdSet("all");
        for (String s : subset)
            logger.info("HELLO " + s);
        Iterator<IndexedCommunication> readIter = reader.loadCommunications(subset);
        while (readIter.hasNext()) {
            IndexedCommunication com = readIter.next();
            logger.info("===================\n" + com);
        }
        reader.close();
        logger.info("All done");
    }

    public static void initialize(Corpus corpus) throws RebarException {
        Corpus.Initializer initializer = corpus.makeInitializer();
        for (int i = 0; i < 10; ++i) {
            String docid = "doc-" + i;
            String text = "this is document " + i + ".";
            Concrete.CommunicationGUID guid = Concrete.CommunicationGUID.newBuilder().setCommunicationId(docid)
                    .setCorpusName(corpus.getName()).build();
            Concrete.Communication com = Concrete.Communication.newBuilder().setText(text).setUuid(IdUtil.generateUUID()).setGuid(guid)
                    .setKnowledgeGraph(Concrete.KnowledgeGraph.newBuilder().setUuid(IdUtil.generateUUID()).build()).build();
            initializer.addCommunication(com);
        }
        initializer.close();
    }

}

/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.rebar.tokenization;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.tift.Tokenizer;
import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.CorpusFactory;
import edu.jhu.rebar.IndexedCommunication;
import edu.jhu.rebar.IndexedSentence;
import edu.jhu.rebar.RebarBackends;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;

public class TiftTokenizer {
    private static final Logger logger = LoggerFactory.getLogger(TiftTokenizer.class);

    // ======================================================================
    // Constants
    // ======================================================================
    private static final String TIFT_STAGE_NAME = "tift_tokens";
    private static final String TIFT_STAGE_DESCRIPTION = "A tokenization for each sentence, containing tokens and tags "
            + "generated by the Tift TwitterTokenizer.";

    // ======================================================================
    // Constructor
    // ======================================================================
    public TiftTokenizer() throws RebarException {

    }

    // ======================================================================
    // Tokenization
    // ======================================================================

    /**
     * Use the Jerboa tokenizer to tokenize all communications in the specified
     * corpus.
     */
    public void tokenizeAllCommunications(Corpus.Reader reader, Corpus.Writer writer) throws RebarException {
        Iterator<IndexedCommunication> comIter = reader.loadCommunications();
        int n = 0;
        while (comIter.hasNext()) {
            IndexedCommunication com = comIter.next();
            addTwitterTokenization(com);
            writer.saveCommunication(com);
            if ((++n % 500) == 0) {
                logger.info("Finished " + n + " comms...");
            }
        }
    }

    /**
     * Run the Jerboa tokenzier on every sentence in the given communication,
     * and add the resulting tokenizations. This modifies the communication, but
     * does not save it; use a CorpusWriter to save it (or call
     * tokenizeAllCommunications).
     */
    public void addTwitterTokenization(IndexedCommunication com) throws RebarException {
        // Tokenize each sentence.
        List<IndexedSentence> sentences = com.getSentences();
        if (sentences.size() == 0) {
            throw new RebarException("No sentences found in comm: " + com.getCommunicationId());
        }
        for (IndexedSentence sentence : sentences) {
            int s = sentence.getTextSpan().getStart();
            int e = sentence.getTextSpan().getEnd();
            String text = com.getText().substring(s, e);
            sentence.addTokenization(Tokenizer.TWITTER.tokenizeToConcrete(text, s));
        }
    }

    // ======================================================================
    // Command-Line Interface
    // ======================================================================
    // eg: runjava JerboaTokenizer my_corpus 1.0 segments:1.0

    public static void main(String[] args) {
        // Parse arguments.
        if (args.length < 2) {
            logger.info("Usage: JerboaTokenizer <corpusname> " + "<dst_stage_version> [<src_stage>...]");
            System.exit(-1);
        }

        //IndexedCommunication comm = new IndexedCommunication(null, null, null);
        //comm.getProto().getLanguageIdCount();

        // We use system.exit here (rather than just returning) to be
        // *sure* that we exit, even if some accumulo thread didn't
        // get closed (e.g., because an exception was raised).
        try {
            String corpusName = args[0];
            String dstStageVersion = args[1];
            List<String> srcStages = Arrays.asList(args).subList(2, args.length);
            tokenize(corpusName, dstStageVersion, srcStages);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        System.exit(0);
    }

    public static void tokenize(String corpusName, String dstStageVersion, List<String> srcStages) throws RebarException, IOException {
        logger.info("Connecting to corpus...");
        CorpusFactory cf = RebarBackends.FILE.getCorpusFactory();
        Corpus corpus = cf.getCorpus(corpusName);

        logger.info("Looking up dependencies...");
        Set<Stage> dependencies = new TreeSet<Stage>();
        for (String srcStage : srcStages)
            dependencies.add(corpus.getStage(srcStage));

        logger.info("Creating output stage...");

        // Delete any old results if they exist.
        Stage outputStage = corpus.makeStage(TIFT_STAGE_NAME, dstStageVersion, dependencies, TIFT_STAGE_DESCRIPTION, true);

        // Create reader & writer
        final Corpus.Reader reader = corpus.makeReader(dependencies);
        final Corpus.Writer writer = corpus.makeWriter(outputStage);

        // Tokenize everything.

        logger.info("Tokenizing communications...");
        new TiftTokenizer().tokenizeAllCommunications(reader, writer);

        logger.info("Shutting down...");
        reader.close();
        writer.close();
        corpus.close();

        logger.info("Done.");
    }
}

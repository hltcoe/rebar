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

import edu.jhu.concrete.Concrete;
import edu.jhu.concrete.util.IdUtil;
import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.CorpusFactory;
import edu.jhu.rebar.IndexedCommunication;
import edu.jhu.rebar.IndexedSentence;
import edu.jhu.rebar.RebarBackends;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;

public class JerboaTokenizer {
    private static final Logger logger = LoggerFactory.getLogger(JerboaTokenizer.class);

    // ======================================================================
    // Constants
    // ======================================================================
    private static final String JERBOA_STAGE_NAME = "jerboa_tokens";
    private static final String JERBOA_TOOL_NAME = "edu.jhu.jerboa.processing.TwitterTokenizer";
    private static final String JERBOA_STAGE_DESCRIPTION = "A tokenization for each sentence, containing tokens and tags "
            + "generated by the Jerboa TwitterTokenizer.";

    // ======================================================================
    // Private Variables
    // ======================================================================
    private final Concrete.AnnotationMetadata jerboaMetadata;

    // ======================================================================
    // Constructor
    // ======================================================================
    public JerboaTokenizer() throws RebarException {
        this.jerboaMetadata = Concrete.AnnotationMetadata.newBuilder().setTool(JERBOA_TOOL_NAME)
        // note: not bothering with a timestamp.
                .build();
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
            addTokenization(com);
            writer.saveCommunication(com);
            if ((++n % 500) == 0) {
                logger.info("  [" + n + "]");
            }
        }
    }

    /**
     * Run the Jerboa tokenzier on every sentence in the given communication,
     * and add the resulting tokenizations. This modifies the communication, but
     * does not save it; use a CorpusWriter to save it (or call
     * tokenizeAllCommunications).
     */
    public void addTokenization(IndexedCommunication com) throws RebarException {
        // Tokenize each sentence.
        List<IndexedSentence> sentences = com.getSentences();
        if (sentences.size() == 0) {
            logger.info("Warning: no sentences found in com " + com.getCommunicationId());
            throw new RebarException("NO SENTENCES FOUND");
        }
        for (IndexedSentence sentence : sentences) {
            int s = sentence.getTextSpan().getStart();
            int e = sentence.getTextSpan().getEnd();
            String text = com.getText().substring(s, e);
            sentence.addTokenization(tokenize(text, s));
        }
    }

    /**
     * Run the Jerboa tokenizer on the given text string, and return a Rebar
     * Tokenization containing Jerboa's results. The returned Tokenization will
     * have kind=TOKEN_LIST. In addition to the tokens, the returned
     * tokenization will also include a corse-grained tagging generated by the
     * tokenization will also include a coarse-grained tagging generated by the
     * Jerboa tagger.
     * 
     * @param startPos
     *            The character offset of the first character in the text
     *            string. This is used to assign appropriate character offsets
     *            to the generated Tokens.
     * 
     *            TODO :: implement me
     */
    public Concrete.Tokenization tokenize(String text, int startPos) throws RebarException {
        // Run jerboa.
        // final String[][] jerboaResult;
        // try {
        // jerboaResult = Tokenizer.TWITTER.tokenize(text);
        // } catch (java.io.IOException e) {
        // throw new RebarException(e);
        // }
        // // Do a sanity check on the results.
        // if ((jerboaResult.length != 3) ||
        // (jerboaResult[0].length != jerboaResult[1].length) ||
        // (jerboaResult[0].length != jerboaResult[2].length))
        // throw new
        // RebarException("Jerboa's TwitterTokenizer returned an array "+
        // "with an unexpected shape.");
        // // Wrap/parse the results to make them easier to read.
        // List<String> tokens = Arrays.asList(jerboaResult[0]);
        // List<String> tagging = Arrays.asList(jerboaResult[1]);
        // List<Integer> offsets = new
        // ArrayList<Integer>(jerboaResult[2].length);
        // for(String s: jerboaResult[2])
        // offsets.add(Integer.parseInt(s));
        // Add the tokens.
        Concrete.Tokenization.Builder tokenizationBuilder = Concrete.Tokenization.newBuilder().setUuid(IdUtil.generateUUID())
                .setMetadata(jerboaMetadata).setKind(Concrete.Tokenization.Kind.TOKEN_LIST);
        // Note: we use token index as token id.
        // for (int tokenId=0; tokenId<tokens.size(); ++tokenId) {
        // String token = tokens.get(tokenId);
        // int start = startPos + offsets.get(tokenId);
        // int end = start + token.length();
        // tokenizationBuilder.addTokenBuilder()
        // .setTokenId(tokenId)
        // .setText(token)
        // .setTextSpan(Concrete.TextSpan.newBuilder()
        // .setStart(start)
        // .setEnd(end));
        // }
        // Add the part of speech tagging.
        // Concrete.TokenTagging.Builder tagsBuilder =
        // tokenizationBuilder.addTaggingBuilder();
        // tagsBuilder.setUuid(IdUtil.generateUUID());
        // tagsBuilder.setMetadata(jerboaMetadata);
        // for (int tokenId=0; tokenId<tokens.size(); ++tokenId) {
        // String tag = tagging.get(tokenId);
        // if (tag != null) {
        // tagsBuilder.addTaggedTokenBuilder()
        // .setTag(tag)
        // .setTokenId(tokenId);
        // }
        // }

        return tokenizationBuilder.build();
    }

    // ======================================================================
    // Command-Line Interface
    // ======================================================================
    // eg: runjava JerboaTokenizer my_corpus 1.0 segments:1.0

    public static void main(String[] args) throws RebarException, IOException {
        // Parse arguments.
        if (args.length < 2) {
            logger.info("Usage: JerboaTokenizer <corpusname> " + "<dst_stage_version> [<src_stage>...]");
            System.exit(-1);
        }

        IndexedCommunication comm = new IndexedCommunication(null, null, null);
        comm.getProto().getLanguageIdCount();

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
        } finally {
            System.exit(0);
        }
    }

    public static void tokenize(String corpusName, String dstStageVersion, List<String> srcStages) throws RebarException, IOException {
        logger.info("Connecting to corpus...");
        CorpusFactory cf = RebarBackends.ACCUMULO.getCorpusFactory();
        Corpus corpus = cf.getCorpus(corpusName);

        logger.info("Looking up dependencies...");
        Set<Stage> dependencies = new TreeSet<Stage>();
        for (String srcStage : srcStages)
            dependencies.add(corpus.getStage(srcStage));

        logger.info("Creating output stage...");

        // Delete any old results if they exist.
        Stage outputStage = corpus.makeStage(JERBOA_STAGE_NAME, dstStageVersion, dependencies, JERBOA_STAGE_DESCRIPTION, true);

        // Create reader & writer
        final Corpus.Reader reader = corpus.makeReader(dependencies);
        final Corpus.Writer writer = corpus.makeWriter(outputStage);

        // Tokenize everything.

        logger.info("Tokenizing communications...");
        new JerboaTokenizer().tokenizeAllCommunications(reader, writer);

        logger.info("Shutting down...");
        reader.close();
        writer.close();
        corpus.close();

        logger.info("All done!...");
    }
}

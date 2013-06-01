/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.rebar.ingest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import edu.jhu.hlt.concrete.Concrete;
import edu.jhu.hlt.concrete.Concrete.KnowledgeGraph;
import edu.jhu.hlt.concrete.Concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.Concrete.Tokenization;
import edu.jhu.hlt.concrete.Concrete.TweetInfo;
import edu.jhu.hlt.tift.ConcreteSectionSegmentation;
import edu.jhu.hlt.tift.Tokenizer;
import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.IndexedCommunication;
import edu.jhu.rebar.IndexedEdge;
import edu.jhu.rebar.IndexedKnowledgeGraph;
import edu.jhu.rebar.IndexedVertex;
import edu.jhu.rebar.NewInitializer;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;
import edu.jhu.rebar.file.FileCorpusFactory;

/** 
 * Processing class for reading a twitter {@link Corpus},
 * and writing the result to corpus {@link Stage}s. Includes a 
 * {@link Tokenization} and an initial {@link KnowledgeGraph}.
 *
 * Adds the following Stages:
 *   tokenization -- tokenization based on the twitter {@link Tokenizer}
 *   com_graph -- initial communication graph, containing vertices for the
 *      communication, the twitter account, and the sender.
 */
public class TwitterCorpusIngester {
    
    private static final Logger logger = LoggerFactory
            .getLogger(TwitterCorpusIngester.class);    

    private final Corpus corpus;

    private final Concrete.AttributeMetadata attribMetadata;
    
    // Tokenization stage
    public static final String TOKENIZATION_STAGE_NAME = "twitter_tokenization";
    public static String TOKENIZATION_STAGE_VERSION = "1.0";
    public static final String TOKENIZATION_STAGE_DESCRIPTION = 
            "Tokenization from the Twitter Tokenizer.";
    private final Stage tokenizationStage;
    private final Corpus.Writer tokenizationWriter;

    // InitialGraph stage
    private final FieldDescriptor GRAPH_EDGE_FIELD = Concrete.KnowledgeGraph
            .getDescriptor().findFieldByName("vertex");
    public static final String INITIAL_GRAPH_STAGE_NAME = "com_graph";
    public static final String INITIAL_GRAPH_STAGE_VERSION = "1.0";
    public static final String INITIAL_GRAPH_STAGE_DESCRIPTION = "An initial communication graph, containing three vertices: one for the "
            + "tweet (Communiation), one for the sender (Person), and one for the "
            + "twitter account used to send the tweet (ComChannel).";
    private final Stage initialGraphStage;
    private final Corpus.Writer initialGraphWriter;

    int tweetsAdded = 0;
    int dupes = 0;

    public TwitterCorpusIngester(Corpus corpus) throws RebarException {
        Set<Stage> noDependencies = new TreeSet<Stage>();
        this.corpus = corpus;

        // Stage writer for sentence segmentation
        if (!this.corpus.hasStage(TOKENIZATION_STAGE_NAME, TOKENIZATION_STAGE_VERSION)) {
            this.tokenizationStage = corpus.makeStage(TOKENIZATION_STAGE_NAME, 
                    TOKENIZATION_STAGE_VERSION, noDependencies, 
                    TOKENIZATION_STAGE_DESCRIPTION, false);
        } else {
            this.tokenizationStage = this.corpus.getStage(TOKENIZATION_STAGE_NAME, 
                    TOKENIZATION_STAGE_VERSION);
        }
        this.tokenizationWriter = corpus.makeWriter(this.tokenizationStage);
        // Stage writer for initial knowledge graph
        if (!this.corpus.hasStage(INITIAL_GRAPH_STAGE_NAME, INITIAL_GRAPH_STAGE_VERSION)) {
            this.initialGraphStage = corpus.makeStage(INITIAL_GRAPH_STAGE_NAME, 
                    INITIAL_GRAPH_STAGE_VERSION, noDependencies,
                    INITIAL_GRAPH_STAGE_DESCRIPTION, false);
        } else {
            this.initialGraphStage = this.corpus.getStage(INITIAL_GRAPH_STAGE_NAME, INITIAL_GRAPH_STAGE_VERSION);
        }
        this.initialGraphWriter = corpus.makeWriter(this.initialGraphStage);
        // Metadata
        this.attribMetadata = Concrete.AttributeMetadata.newBuilder()
                .setTool("edu.jhu.rebar.ingest.TwitterCorpusIngester")
                .setConfidence(1.0f).build();

        // Sanity checks
        if (COM_GUID == null)
            throw new RebarException("COM_GUID attribute not found");
        if (TWITTER_ID == null)
            throw new RebarException("TWITTER_ID attribute not found");
        if (TWITTER_HANDLE == null)
            throw new RebarException("TWITTER_HANDLE attribute not found");
        if (IS_SENDER == null)
            throw new RebarException("IS_SENDER attribute not found");
        if (USES == null)
            throw new RebarException("USES attribute not found");
        if (COMS_SENT == null)
            throw new RebarException("COMS_SENT attribute not found");
    }

    public void close() throws RebarException {
        this.tokenizationWriter.close();
        this.initialGraphWriter.close();
    }

    private void addSectionSegmentation(IndexedCommunication com) throws RebarException {
        String text = com.getText();
        SectionSegmentation ss = 
                ConcreteSectionSegmentation
                .generateSectionSegmentation(Tokenizer.TWITTER,  text);
        com.addSectionSegmentation(ss);
        tokenizationWriter.saveCommunication(com);
    }

    // Constants
    private final static Descriptor VERTEX_DESCRIPTOR = Concrete.Vertex.getDescriptor();
    private final static FieldDescriptor VERTEX_KIND = VERTEX_DESCRIPTOR.findFieldByName("kind");
    private final static FieldDescriptor COM_GUID = VERTEX_DESCRIPTOR.findFieldByName("communication_guid");
    private final static FieldDescriptor TWITTER_ID = VERTEX_DESCRIPTOR.findFieldByName("twitter_id");
    private final static FieldDescriptor TWITTER_HANDLE = VERTEX_DESCRIPTOR.findFieldByName("twitter_handle");
    private final static Descriptor DIRECTED_ATTRIBUTES_DESCRIPTOR = Concrete.DirectedAttributes.getDescriptor();
    private final static FieldDescriptor IS_SENDER = DIRECTED_ATTRIBUTES_DESCRIPTOR.findFieldByName("is_sender");
    private final static FieldDescriptor USES = DIRECTED_ATTRIBUTES_DESCRIPTOR.findFieldByName("uses");
    private final static FieldDescriptor COMS_SENT = DIRECTED_ATTRIBUTES_DESCRIPTOR.findFieldByName("num_coms_sent");

    private void addInitialGraph(IndexedCommunication com) throws RebarException {
        IndexedKnowledgeGraph graph = com.getKnowledgeGraph();
        TweetInfo tweet = com.getProto().getTweetInfo();
        // Create three vertices: one for the tweet, one for the
        // person who sent it, and one for the account that was used
        // to send it.
        IndexedVertex senderVertex = graph.addVertex();
        senderVertex.addAttribute(VERTEX_KIND, Concrete.Vertex.Kind.PERSON, attribMetadata);
        IndexedVertex tweetVertex = graph.addVertex();
        tweetVertex.addAttribute(VERTEX_KIND, Concrete.Vertex.Kind.COMMUNICATION, attribMetadata);
        tweetVertex.addAttribute(COM_GUID, com.getGuid(), attribMetadata);
        IndexedVertex accountVertex = graph.addVertex();
        accountVertex.addAttribute(VERTEX_KIND, Concrete.Vertex.Kind.COM_CHANNEL, attribMetadata);
        accountVertex.addAttribute(TWITTER_ID, tweet.getUser().getId(), attribMetadata);
        accountVertex.addAttribute(TWITTER_HANDLE, tweet.getUser().getScreenName(), attribMetadata);

        // Edge between the sender and the message that they sent.
        IndexedEdge senderTweetEdge = graph.getEdge(senderVertex, tweetVertex);
        senderTweetEdge.addDirectedAttribute(senderVertex, IS_SENDER, tweetVertex, true, attribMetadata);

        // Edge between the sender and the account they used.
        IndexedEdge senderAccountEdge = graph.getEdge(senderVertex, accountVertex);
        senderAccountEdge.addDirectedAttribute(senderVertex, USES, accountVertex, true, attribMetadata);
        senderAccountEdge.addDirectedAttribute(senderVertex, COMS_SENT, accountVertex, 1, attribMetadata);

        // Edge between the message and the account used to send it.
        IndexedEdge accountTweetEdge = graph.getEdge(accountVertex, tweetVertex);
        accountTweetEdge.addDirectedAttribute(accountVertex, IS_SENDER, tweetVertex, true, attribMetadata);

        // Save our changes.
        initialGraphWriter.saveCommunication(com);
    }

    public void addTokenizationStage() throws RebarException {
        Corpus.Reader commReader = this.corpus.makeReader();
        Iterator<IndexedCommunication> icIter = commReader.loadCommunications();
        while (icIter.hasNext()) {
            IndexedCommunication ic = icIter.next();
            addSectionSegmentation(ic);
        }
        
        commReader.close();
    }
    
    public void addGraphStage() throws RebarException {
        Corpus.Reader commReader = this.corpus.makeReader();
        Iterator<IndexedCommunication> icIter = commReader.loadCommunications();
        while (icIter.hasNext()) {
            IndexedCommunication ic = icIter.next();
            addInitialGraph(ic);
        }
        
        commReader.close();
    }

    public static void main(String[] args) throws RebarException, IOException, FileNotFoundException {
        if (args.length < 2) {
            logger.info("Usage: TwitterCorpusIngester <corpusname> <filename>...");
            return;
        }

        logger.info("Starting.");
        // Thread.sleep(10000);
        String corpusName = args[0];
        FileCorpusFactory cf = new FileCorpusFactory();
        if (cf.corpusExists(corpusName)) {
            logger.info("Deleting existing corpus: " + corpusName);
            cf.deleteCorpus(corpusName);
        }
        
        logger.info("Creating initializer: " + corpusName);
        NewInitializer init = cf.createCorpusInitializer(corpusName);
        
        //logger.info("Creating corpus: " + corpusName);
        //Corpus corpus = cf.makeCorpus(corpusName);
        
        logger.info("Ingesting twitter files...");
        TwitterCommunicationsIngester ingester = 
                new TwitterCommunicationsIngester(init);
        for (int i = 1; i < args.length; i++) {
            ingester.ingestFile(new File(args[i]));
        }
        
        ingester.finishIngest();
        logger.info("Tweets ingested: " + ingester.getTweetsAdded());
        init.close();
        
        Corpus c = cf.getCorpus(corpusName);
        TwitterCorpusIngester tci = new TwitterCorpusIngester(c);
        tci.addTokenizationStage();
        tci.addGraphStage();
        tci.close();
    }
}

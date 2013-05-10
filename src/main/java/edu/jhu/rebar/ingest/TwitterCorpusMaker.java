/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.rebar.ingest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.protobuf.Descriptors.FieldDescriptor;

import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;
import edu.jhu.concrete.Concrete;


/** Processing class for reading a twitter corpus from a set of files,
 * and writing the result to a Corpus.
 *
 * Adds the following Stages:
 *   root -- contains guid, text, date
 *   tweet_info -- contains all tweet info
 *   default_section_segmentation -- contains a single section spanning the entire tweet
 *   default_sentence_segmentation -- contains a single sentence spanning the entire tweet
 *   com_graph -- initial communication graph, containing vertices for the
 *      communication, the twitter account, and the sender.
 */

public class TwitterCorpusMaker {
	private static final Logger LOGGER = Logger.getLogger(TwitterCorpusMaker.class);

	private final Corpus corpus;
	private final Corpus.Initializer initializer;
	private final Concrete.AnnotationMetadata annotationMetadata;
	private final Concrete.AttributeMetadata attribMetadata;

	// TweetInfo stage
	private final FieldDescriptor TWEET_INFO_FIELD = Concrete.Communication.getDescriptor().findFieldByName("tweet_info");
	private final String TWEET_INFO_STAGE_NAME = "tweet_info";
	private final String TWEET_INFO_STAGE_VERSION = "1.0";
	private final String TWEET_INFO_STAGE_DESCRIPTION =
			"Information about this tweet that is provided by the standard Twitter JSON API.";
	private final Stage tweetInfoStage;

	// Section Segmentation Stage
	private final String SEC_SEG_STAGE_NAME = "default_section_segmentation";
	private final String SEC_SEG_STAGE_VERSION = "1.0";
	private final String SEC_SEG_STAGE_DESCRIPTION =
			"A segmentation consisting of a single section, spanning the entire tweet.";
	private final Stage secSegStage;

	// Section Segmentation Stage
	private final String SENT_SEG_STAGE_NAME = "default_sentence_segmentation";
	private final String SENT_SEG_STAGE_VERSION = "1.0";
	private final String SENT_SEG_STAGE_DESCRIPTION =
			"A segmentation consisting of a single sentence, spanning the entire tweet.";
	private final Stage sentSegStage;

	// InitialGraph stage
	private final FieldDescriptor GRAPH_EDGE_FIELD = Concrete.KnowledgeGraph.getDescriptor().findFieldByName("vertex");
	private final String INITIAL_GRAPH_STAGE_NAME = "com_graph";
	private final String INITIAL_GRAPH_STAGE_VERSION = "1.0";
	private final String INITIAL_GRAPH_STAGE_DESCRIPTION =
			"An initial communication graph, containing three vertices: one for the "+
					"tweet (Communiation), one for the sender (Person), and one for the "+
					"twitter account used to send the tweet (ComChannel).";
	private final Stage initialGraphStage;

	private boolean duplicatesDetected = false;

	public TwitterCorpusMaker(Corpus corpus) throws RebarException {
		List<Stage> noDependencies = new ArrayList<Stage>();
		this.corpus = corpus;
		// For writing the root communication objects:
		this.initializer = corpus.makeInitializer();
		// Stage for tweet information (from twitter api)
		if (!this.corpus.hasStage(TWEET_INFO_STAGE_NAME, TWEET_INFO_STAGE_VERSION)) {
			this.tweetInfoStage = corpus.makeStage(
					TWEET_INFO_STAGE_NAME, TWEET_INFO_STAGE_VERSION,
					noDependencies, TWEET_INFO_STAGE_DESCRIPTION,
					false);} else {
						this.tweetInfoStage = this.corpus.getStage(TWEET_INFO_STAGE_NAME, TWEET_INFO_STAGE_VERSION); }

		// Stage for section segmentation
		if (!this.corpus.hasStage(SEC_SEG_STAGE_NAME, SEC_SEG_STAGE_VERSION)) {
			this.secSegStage = corpus.makeStage(
					SEC_SEG_STAGE_NAME, SEC_SEG_STAGE_VERSION,
					noDependencies, SEC_SEG_STAGE_DESCRIPTION,
					false);} else {
						this.secSegStage = this.corpus.getStage(SEC_SEG_STAGE_NAME, SEC_SEG_STAGE_VERSION);}

		// Stage for sentence segmentation
		if (!this.corpus.hasStage(SENT_SEG_STAGE_NAME, SENT_SEG_STAGE_VERSION)) {
			List<Stage> sentSegDeps = new ArrayList<Stage>();
			sentSegDeps.add(this.secSegStage);
			this.sentSegStage = corpus.makeStage(
					SENT_SEG_STAGE_NAME, SENT_SEG_STAGE_VERSION,
					sentSegDeps, SENT_SEG_STAGE_DESCRIPTION,
					false);} else {
						this.sentSegStage = this.corpus.getStage(SENT_SEG_STAGE_NAME, SENT_SEG_STAGE_VERSION);}

		// Stage for initial knowledge graph
		if (!this.corpus.hasStage(INITIAL_GRAPH_STAGE_NAME, INITIAL_GRAPH_STAGE_VERSION)) {
			this.initialGraphStage = corpus.makeStage(
					INITIAL_GRAPH_STAGE_NAME, INITIAL_GRAPH_STAGE_VERSION,
					noDependencies, INITIAL_GRAPH_STAGE_DESCRIPTION,
					false);} else {
						this.initialGraphStage = this.corpus.getStage(INITIAL_GRAPH_STAGE_NAME, INITIAL_GRAPH_STAGE_VERSION);}

		// Metadata
		this.attribMetadata = Concrete.AttributeMetadata.newBuilder()
				.setTool("jhu.hltcoe.rebar2.ingest.TwitterCorpusIngester")
				.setConfidence(1.0f)
				.build();
		this.annotationMetadata = Concrete.AnnotationMetadata.newBuilder()
				.setTool("jhu.hltcoe.rebar2.ingest.TwitterCorpusIngester")
				//.setTimestamp()
				.build();
	}

	void close() throws RebarException {
		LOGGER.info("Closing initializer");
		this.initializer.close();
		LOGGER.info("Marking stages as public");
		corpus.markStagePublic(tweetInfoStage);
		corpus.markStagePublic(secSegStage);
		corpus.markStagePublic(sentSegStage);
		corpus.markStagePublic(initialGraphStage);
	}

	public static void main(String[] args) throws RebarException, IOException, FileNotFoundException  {
		if (args.length != 1) {
			LOGGER.info("Usage: TwitterCorpusMaker <corpusname>");
			return;
		}

		try {
			String corpusName = args[0];
			if (Corpus.Factory.corpusExists(corpusName)) {
				LOGGER.warn("Corpus already exists "+corpusName);
			} else {
				LOGGER.info("Creating corpus "+corpusName);
				Corpus.Factory.makeCorpus(corpusName);
				Corpus corpus = Corpus.Factory.getCorpus(corpusName);
				TwitterCorpusMaker stages = new TwitterCorpusMaker(corpus);
				LOGGER.info("Closing corpus");
				stages.close();
			}
		} catch (Exception e) {
			// We use system.exit here (rather than just returning) to
			// be *sure* that we exit, even if some accumulo thread
			// didn't get closed (e.g., because an exception was
			// raised).
			e.printStackTrace();
			System.exit(-1);
		} finally {
			System.exit(0);
		}
	}
}

/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.hlt.rebar.ingest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.protobuf.Descriptors.FieldDescriptor;

import edu.jhu.hlt.concrete.Concrete;
import edu.jhu.hlt.concrete.ConcreteException;
import edu.jhu.hlt.concrete.Twitter;
import edu.jhu.hlt.concrete.index.IndexedCommunication;
import edu.jhu.hlt.concrete.index.IndexedSection;
import edu.jhu.hlt.concrete.util.IdUtil;
import edu.jhu.hlt.rebar.Corpus;
import edu.jhu.hlt.rebar.CorpusFactory;
import edu.jhu.hlt.rebar.RebarBackends;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Stage;
import edu.jhu.hlt.rebar.file.FileCorpusFactory;
import edu.jhu.hlt.rebar.util.FileUtil;

/**
 * Processing class for reading a twitter corpus from a set of files, and writing the result to a Corpus.
 * 
 * Adds the following Stages: root -- contains guid, text, date tweet_info -- contains all tweet info default_section_segmentation -- contains a single section
 * spanning the entire tweet default_sentence_segmentation -- contains a single sentence spanning the entire tweet com_graph -- initial communication graph,
 * containing vertices for the communication, the twitter account, and the sender.
 */
public class TwitterCorpusIngester {

  private static final Logger logger = LoggerFactory.getLogger(TwitterCorpusIngester.class);

  private static final Set<Long> dupeIds = new HashSet<>();

  private final Corpus corpus;
  private final Corpus.Initializer initializer;
  private final Concrete.AnnotationMetadata annotationMetadata;

  // TweetInfo stage
  private final FieldDescriptor TWEET_INFO_FIELD = Concrete.Communication.getDescriptor().findFieldByName("tweet_info");
  private final String TWEET_INFO_STAGE_NAME = "tweet_info";
  private final String TWEET_INFO_STAGE_VERSION = "1.0";
  private final String TWEET_INFO_STAGE_DESCRIPTION = "Information about this tweet that is provided by the standard Twitter JSON API.";
  private final Stage tweetInfoStage;
  private final Corpus.Writer tweetInfoWriter;

  // Section Segmentation Stage
  private final String SEC_SEG_STAGE_NAME = "default_section_segmentation";
  private final String SEC_SEG_STAGE_VERSION = "1.0";
  private final String SEC_SEG_STAGE_DESCRIPTION = "A segmentation consisting of a single section, spanning the entire tweet.";
  private final Stage secSegStage;
  private final Corpus.Writer secSegWriter;

  // Section Segmentation Stage
  private final String SENT_SEG_STAGE_NAME = "default_sentence_segmentation";
  private final String SENT_SEG_STAGE_VERSION = "1.0";
  private final String SENT_SEG_STAGE_DESCRIPTION = "A segmentation consisting of a single sentence, spanning the entire tweet.";
  private final Stage sentSegStage;
  private final Corpus.Writer sentSegWriter;

  int tweetsAdded = 0;
  int dupes = 0;

  public TwitterCorpusIngester(Corpus corpus) throws RebarException {
    Set<Stage> noDependencies = new TreeSet<Stage>();
    this.corpus = corpus;
    // For writing the root communication objects:
    this.initializer = corpus.makeInitializer();
    // Stage writer for tweet information (from twitter api)
    if (!this.corpus.hasStage(TWEET_INFO_STAGE_NAME, TWEET_INFO_STAGE_VERSION)) {
      this.tweetInfoStage = corpus.makeStage(TWEET_INFO_STAGE_NAME, TWEET_INFO_STAGE_VERSION, noDependencies, TWEET_INFO_STAGE_DESCRIPTION, false);
    } else {
      this.tweetInfoStage = this.corpus.getStage(TWEET_INFO_STAGE_NAME, TWEET_INFO_STAGE_VERSION);
    }
    this.tweetInfoWriter = corpus.makeWriter(this.tweetInfoStage);
    // Stage writer for section segmentation
    if (!this.corpus.hasStage(SEC_SEG_STAGE_NAME, SEC_SEG_STAGE_VERSION)) {
      this.secSegStage = corpus.makeStage(SEC_SEG_STAGE_NAME, SEC_SEG_STAGE_VERSION, noDependencies, SEC_SEG_STAGE_DESCRIPTION, false);
    } else {
      this.secSegStage = this.corpus.getStage(SEC_SEG_STAGE_NAME, SEC_SEG_STAGE_VERSION);
    }
    this.secSegWriter = corpus.makeWriter(this.secSegStage);
    // Stage writer for sentence segmentation
    if (!this.corpus.hasStage(SENT_SEG_STAGE_NAME, SENT_SEG_STAGE_VERSION)) {
      Set<Stage> sentSegDeps = new TreeSet<Stage>();
      sentSegDeps.add(this.secSegStage);
      this.sentSegStage = corpus.makeStage(SENT_SEG_STAGE_NAME, SENT_SEG_STAGE_VERSION, sentSegDeps, SENT_SEG_STAGE_DESCRIPTION, false);
    } else {
      this.sentSegStage = this.corpus.getStage(SENT_SEG_STAGE_NAME, SENT_SEG_STAGE_VERSION);
    }
    this.sentSegWriter = corpus.makeWriter(this.sentSegStage);

    this.annotationMetadata = Concrete.AnnotationMetadata.newBuilder().setTool("jhu.hltcoe.rebar2.ingest.TwitterCorpusIngester")
    // .setTimestamp()
        .build();
  }

  void close() throws RebarException {
    logger.info("Closing writers");
    this.initializer.close();
    this.tweetInfoWriter.close();
    this.secSegWriter.close();
    this.sentSegWriter.close();
    logger.info("Marking stages as public");
    corpus.markStagePublic(tweetInfoStage);
    corpus.markStagePublic(secSegStage);
    corpus.markStagePublic(sentSegStage);
  }

  private static final DateTimeFormatter tweetDateFormat = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z YYYY");

  private IndexedCommunication addRoot(Twitter.TweetInfo tweet) throws RebarException {
    // Use the tweet id as the document id.
    String docid = new Long(tweet.getId()).toString();
    Concrete.CommunicationGUID guid = Concrete.CommunicationGUID.newBuilder().setCommunicationId(docid).setCorpusName(corpus.getName()).build();

    // Create the basic communication with the tweet text
    Concrete.Communication.Builder comBuilder = Concrete.Communication.newBuilder().setUuid(IdUtil.generateUUID()).setGuid(guid).setText(tweet.getText())
        .setKind(Concrete.Communication.Kind.TWEET);
    // Add the start time/date, if we have it.
    if (tweet.hasCreatedAt()) {
      try {
        DateTime dateTime = tweetDateFormat.parseDateTime(tweet.getCreatedAt());
        comBuilder.setStartTime(dateTime.getMillis() / 1000);
      } catch (IllegalArgumentException iae) {
        logger.debug("Tweet " + docid + " had a malformed date. Skipping date creation.");
      }
    }
    // Build it & add it to the corpus.
    return initializer.addCommunication(comBuilder.build());
  }

  private void addTweetInfo(IndexedCommunication com, Twitter.TweetInfo tweet) throws RebarException {
    try {
      com.setField(TWEET_INFO_FIELD, tweet);
      tweetInfoWriter.saveCommunication(com);
    } catch (ConcreteException e) {
      throw new RebarException(e);
    }
  }

  private void addSectionSegmentation(IndexedCommunication com) throws RebarException {
    String text = com.getText();
    // Wrap it in a simple segmentation.
    Concrete.TextSpan textSpan = Concrete.TextSpan.newBuilder().setStart(0).setEnd(text.length()).build();
    Concrete.SectionSegmentation.Builder segmentation = Concrete.SectionSegmentation.newBuilder();
    segmentation.setUuid(IdUtil.generateUUID()).setMetadata(annotationMetadata).addSectionBuilder() 
        .setUuid(IdUtil.generateUUID()).setTextSpan(textSpan);
    try {
      com.addSectionSegmentation(segmentation.build());
      secSegWriter.saveCommunication(com);
    } catch (ConcreteException e) {
      throw new RebarException(e);
    }
  }

  private void addSentenceSegmentation(IndexedCommunication com) throws RebarException {
    try {
      for (IndexedSection sec : com.getSections()) {
        Concrete.SentenceSegmentation.Builder segmentation = Concrete.SentenceSegmentation.newBuilder();
        segmentation.setUuid(IdUtil.generateUUID()).setMetadata(annotationMetadata).addSentenceBuilder() 
            .setTextSpan(sec.getTextSpan()).setUuid(IdUtil.generateUUID());
        sec.addSentenceSegmentation(segmentation.build());
      }
      sentSegWriter.saveCommunication(com);
    } catch (ConcreteException e) {
      throw new RebarException(e);
    }
  }

  private void ingestTweet(Twitter.TweetInfo tweet) throws RebarException {
    long tweetId = tweet.getId();
    logger.debug("Tweet id: " + tweetId);

    if (dupeIds.contains(tweetId)) {
      logger.debug("In our tweet ID set.");
      this.dupes++;
      return;
    }

    else
      logger.debug("Not in our tweet ID set.");

    if (this.initializer.communicationExists(Long.toString(tweetId))) {
      logger.debug("Tweet " + tweetId + " has been ingested previously. Skipping.");
      this.dupes++;
      return;
    }
    // this.initialGraphWriter.

    IndexedCommunication com = addRoot(tweet);
    addTweetInfo(com, tweet);
    addSectionSegmentation(com);
    addSentenceSegmentation(com);

    dupeIds.add(tweetId);
    this.tweetsAdded++;
  }

  private void ingest(File filename) throws RebarException, IOException, FileNotFoundException {
    InputStream is = FileUtil.getInputStream(filename);
    Scanner sc = new Scanner(is, "UTF-8");
    logger.info("Ingesting " + filename + "...");
    long lineno = 0;
    while (sc.hasNextLine()) {
      try {
        String line = sc.nextLine();
        lineno++;
        Twitter.TweetInfo tweet = TweetInfoJsonReader.parseJson(line);
        if (tweet.hasText())
          ingestTweet(tweet);

        if (lineno % 10000 == 0)
          logger.info("On line: " + lineno);
      } catch (JsonParseException jpe) {
        logger.error("Couldn't parse this json: line " + lineno + ": " + jpe.getMessage());
      } catch (JsonMappingException jme) {
        logger.error("Couldn't parse this json: line " + lineno + ": " + jme.getMessage());
      }
    }

    sc.close();
    is.close();
  }

  public int getDupes() {
    return this.dupes;
  }

  public int getTweetsAdded() {
    return this.tweetsAdded;
  }

  public static void main(String[] args) throws RebarException, IOException, FileNotFoundException {
    if (args.length < 2) {
      logger.info("Usage: TwitterCorpusIngester <corpusname> <filename>...");
      return;
    }

    logger.info("Starting.");
    // Thread.sleep(10000);
    String corpusName = args[0];
    CorpusFactory cf = RebarBackends.FILE.getCorpusFactory();
    if (cf.corpusExists(corpusName)) {
      logger.info("Deleting existing corpus: " + corpusName);
      new FileCorpusFactory("target/").deleteCorpus(corpusName);
    } else {
      logger.error("Corpus " + corpusName + " does not exist. Creating it.");
    }

    Corpus corpus = cf.makeCorpus(corpusName);
    logger.info("Ingesting twitter files...");
    TwitterCorpusIngester ingester = new TwitterCorpusIngester(corpus);
    for (int i = 1; i < args.length; i++) {
      ingester.ingest(new File(args[i]));
    }
    ingester.close();
    logger.info("Closing corpus.");
    logger.info("Tweets ingested: " + ingester.getTweetsAdded());
    logger.info("Dupes: " + ingester.getDupes());
    corpus.close();
  }
}

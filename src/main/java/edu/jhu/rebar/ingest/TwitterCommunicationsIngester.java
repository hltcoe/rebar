/**
 * 
 */
package edu.jhu.rebar.ingest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import edu.jhu.hlt.concrete.Concrete;
import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.util.IdUtil;
import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.NewInitializer;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.file.FileCorpusFactory;
import edu.jhu.rebar.util.FileUtil;

/**
 * @author max
 *
 */
public class TwitterCommunicationsIngester {
    
    private final NewInitializer init;
    private final String corpusName;
    
    private static final Logger logger = LoggerFactory
            .getLogger(TwitterCommunicationsIngester.class);
    
    private static final DateTimeFormatter tweetDateFormat = 
            DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z YYYY");
    
    private int tweetsAdded = 0;
    
    /**
     * 
     */
    public TwitterCommunicationsIngester(NewInitializer init) {
        this.init = init;
        this.corpusName = this.init.getCorpusName();
    }
    
    private void ingestTweet(Concrete.TweetInfo tweet) throws RebarException {
        long tweetId = tweet.getId();
        logger.trace("Tweet id: " + tweetId);

        // Use the tweet id as the document id.
        String docid = new Long(tweetId).toString();
        // if we hve this tweet already, return and move on. 
        if (this.init.communicationExists(docid))
            return;
        
        Concrete.CommunicationGUID guid = Concrete.CommunicationGUID.newBuilder()
                .setCommunicationId(docid)
                .setCorpusName(this.corpusName)
                .build();
        // Create an empty knowledge graph and assign it a uuid.
        Concrete.KnowledgeGraph graph = Concrete.KnowledgeGraph.newBuilder()
                .setUuid(IdUtil.generateUUID())
                .build();
        // Create the basic communication with the tweet text
        Concrete.Communication.Builder comBuilder = Concrete.Communication.newBuilder()
                .setUuid(IdUtil.generateUUID())
                .setGuid(guid)
                .setText(tweet.getText())
                .setKind(Concrete.Communication.Kind.TWEET)
                .setKnowledgeGraph(graph);
        // Add the start time/date, if we have it.
        if (tweet.hasCreatedAt()) {
            try {
                DateTime dateTime = tweetDateFormat.parseDateTime(tweet.getCreatedAt());
                comBuilder.setStartTime(dateTime.getMillis() / 1000);
            } catch (IllegalArgumentException iae) {
                logger.trace("Tweet " + docid + " had a malformed date. Skipping date creation.");
            }
        }
        
        Communication comm = comBuilder.build();
        this.init.ingest(comm);
        this.tweetsAdded++;
    }
    
    public Corpus finishIngest() throws RebarException {
        return this.init.initialize();
    }
    
    public int getTweetsAdded() {
        return this.tweetsAdded;
    }
    
    public void ingestFile(File filename) throws RebarException {
        try {
            InputStream is = FileUtil.getInputStream(filename);
            Scanner sc = new Scanner(is, "UTF-8");
            logger.info("Ingesting " + filename + "...");
            long lineno = 0;
            while (sc.hasNextLine()) {
                try {
                    String line = sc.nextLine();
                    lineno++;
                    Concrete.TweetInfo tweet = TweetInfoJsonReader.parseJson(line);
                    if (tweet.hasText())
                        ingestTweet(tweet);

                    if (lineno % 10000 == 0)
                        logger.info("On line: " + lineno);
                } catch (JsonParseException | JsonMappingException jpe) {
                    logger.debug("Couldn't parse this json: line " + lineno + ": " + jpe.getMessage());
                }
            }

            sc.close();
            is.close();
        } catch (IOException e) {
            throw new RebarException(e);
        }
    }

    /**
     * @param args
     * @throws RebarException 
     */
    public static void main(String[] args) throws RebarException {
        if (args.length < 2) {
            logger.info("Usage: TwitterCorpusIngester <corpusname> <filename>...");
            return;
        }

        logger.info("Starting.");
        String corpusName = args[0];
        FileCorpusFactory cf = new FileCorpusFactory();
        if (cf.corpusExists(corpusName)) {
            logger.info("Deleting existing corpus: " + corpusName);
            cf.deleteCorpus(corpusName);
        }
        
        logger.info("Creating initializer: " + corpusName);
        NewInitializer init = cf.createCorpusInitializer(corpusName);
        
        logger.info("Ingesting twitter files...");
        TwitterCommunicationsIngester ingester = 
                new TwitterCommunicationsIngester(init);
        for (int i = 1; i < args.length; i++) {
            ingester.ingestFile(new File(args[i]));
        }
        
        ingester.finishIngest();
        logger.info("Tweets ingested: " + ingester.getTweetsAdded());
        init.close();
    }
}

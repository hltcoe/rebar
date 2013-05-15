/**
 * 
 */
package edu.jhu.rebar.ingest.twitter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import edu.jhu.concrete.Concrete;
import edu.jhu.concrete.util.IdUtil;
import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.ingest.TweetInfoJsonReader;
import edu.jhu.rebar.riak.RiakCommunication;
import edu.jhu.rebar.util.FileUtil;

/**
 * @author max
 * 
 */
public class TwitterCommunicationParser {
    
    private static final Logger logger = Logger.getLogger(TwitterCommunicationParser.class);
    
    private static final DateTimeFormatter tweetDateFormat = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z YYYY");
    
    private String nameOfCorpusToIngestTo;
    
    /**
     * 
     */
    public TwitterCommunicationParser(String nameOfCorpusToIngestTo) {
        this.nameOfCorpusToIngestTo = nameOfCorpusToIngestTo;
    }
    
    public TwitterCommunicationParser() {
        this("tweets");
    }
    
    public TwitterCommunicationParser(Corpus corpusToIngestTo) {
        this.nameOfCorpusToIngestTo = corpusToIngestTo.getName();
    }

    public RiakCommunication parseTweetFromTwitterAPIJsonString(String lineOfJson) throws JsonParseException, JsonMappingException, IOException {
            Concrete.TweetInfo tweetInfo = TweetInfoJsonReader.parseJson(lineOfJson);
            // Use the tweet id as the document id.
            String docid = Long.toString(tweetInfo.getId());
            Concrete.CommunicationGUID guid = Concrete.CommunicationGUID.newBuilder()
                    .setCommunicationId(docid)
                    .setCorpusName(nameOfCorpusToIngestTo)
                    .build();
            // Create an empty knowledge graph and assign it a uuid.
            Concrete.KnowledgeGraph graph = Concrete.KnowledgeGraph.newBuilder()
                    .setUuid(IdUtil.generateUUID())
                    .build();
            // Create the basic communication with the tweet text
            Concrete.Communication.Builder comBuilder = Concrete.Communication.newBuilder()
                    .setUuid(IdUtil.generateUUID())
                    .setGuid(guid)
                    .setText(tweetInfo.getText())
                    .setKind(Concrete.Communication.Kind.TWEET)
                    .setKnowledgeGraph(graph);
            // Add the start time/date, if we have it.
            if (tweetInfo.hasCreatedAt()) {
                try {
                    DateTime dateTime = tweetDateFormat.parseDateTime(tweetInfo.getCreatedAt());
                    comBuilder.setStartTime(dateTime.getMillis() / 1000);
                } catch (IllegalArgumentException iae) {
                    logger.debug("Tweet " + docid + " had a malformed date. Skipping date creation.");
                }
            }
            // Build it & return
            return new RiakCommunication(comBuilder.build());
        
    }
    
    public Set<RiakCommunication> parseTwitterAPIFile (String pathToFileWithOneJsonStringPerLine) 
            throws RebarException {
        File f = new File(pathToFileWithOneJsonStringPerLine);
        InputStream is = null;
        try {
            is = FileUtil.getInputStream(f);
            return parseTwitterAPIFile(is);
        } catch (IOException e) {
            throw new RebarException(e);
        } finally {
            try {
                if (is != null)
                    is.close();
            } catch (IOException e) {
                logger.trace(e, e);
            }
        }
    }

    public Set<RiakCommunication> parseTwitterAPIFile (InputStream is) 
            throws FileNotFoundException, RebarException {
        Set<RiakCommunication> comms = new HashSet<>();

        int counter = 0;
        Scanner sc = new Scanner(is, "UTF-8");
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            try {
                comms.add(parseTweetFromTwitterAPIJsonString(line));
                counter++;
            } catch (IOException e) {
                logger.debug("Failed to parse tweet: " + line);
            }
            if (counter == 100000)
                break;
        }
        
        sc.close();
        return comms;
    }
}

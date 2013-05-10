package edu.jhu.rebar.ingest.twitter;

import java.io.IOException;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.riak.RiakCommunication;

public class TwitterCommunicationParserTest {

    private static final Logger logger = Logger.getLogger(TwitterCommunicationParserTest.class);
    
    TwitterCommunicationParser tcp = new TwitterCommunicationParser();
    
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testParseTweetFromTwitterAPIJsonString() throws RebarException, JsonParseException, JsonMappingException, IOException {
        Scanner sc = new Scanner(TwitterCommunicationParser.class.getClassLoader().getResourceAsStream("1k-tweets.txt"));
        while (sc.hasNextLine()) {
            String line = sc.nextLine().trim();
            RiakCommunication rc = this.tcp.parseTweetFromTwitterAPIJsonString(line);
            logger.debug(rc.toString());
        }
        
        sc.close();
    }

}

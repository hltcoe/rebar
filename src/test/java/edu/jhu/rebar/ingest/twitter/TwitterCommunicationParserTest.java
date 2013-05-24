package edu.jhu.rebar.ingest.twitter;

import static org.junit.Assert.*;

import java.io.IOException;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.riak.RiakCommunication;
import edu.jhu.rebar.util.TestTweets;

public class TwitterCommunicationParserTest {
    
    TwitterCommunicationParser tcp = new TwitterCommunicationParser();
    
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testParseTweetFromTwitterAPIJsonString() throws RebarException, JsonParseException, JsonMappingException, IOException {
    	RiakCommunication rc = this.tcp.parseTweetFromTwitterAPIJsonString(TestTweets.TWEET);
    	assertEquals("327330790637703168", rc.getGuid());
    }

}

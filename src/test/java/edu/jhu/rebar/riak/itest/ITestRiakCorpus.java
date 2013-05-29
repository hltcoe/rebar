package edu.jhu.rebar.riak.itest;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.rebar.riak.RiakCorpus;

public class ITestRiakCorpus {

    RiakCorpus rc;
    
    @Before
    public void setUp() throws Exception {
//        this.rc = Corpus.Instances.RIAK.getCorpus("test");
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testMakeWriter() {
        fail("Not yet implemented");
    }

}

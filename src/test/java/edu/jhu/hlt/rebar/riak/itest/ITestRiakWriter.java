/**
 * 
 */
package edu.jhu.hlt.rebar.riak.itest;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.Concrete.CommunicationGUID;
import edu.jhu.hlt.concrete.Concrete.KnowledgeGraph;
import edu.jhu.hlt.concrete.Concrete.UUID;
import edu.jhu.hlt.concrete.util.IdUtil;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.riak.RiakCommunication;
import edu.jhu.hlt.rebar.riak.RiakWriter;

/**
 * @author max
 *
 */
public class ITestRiakWriter {
    private static final Logger logger = Logger.getLogger(ITestRiakWriter.class);
    
    RiakWriter rw;
    
    List<String> fetchedIdsToDelete;
    
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        this.rw = new RiakWriter();
        this.fetchedIdsToDelete = new ArrayList<>();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        IRiakClient irc = RiakFactory.pbcClient();
        Bucket b = irc.fetchBucket("comms").execute();
        for (String s : this.fetchedIdsToDelete)
            b.delete(s).execute();
        irc.shutdown();
        rw.close();
    }

    /**
     * Test method for {@link edu.jhu.hlt.rebar.riak.RiakWriter#write(edu.jhu.hlt.rebar.riak.RiakCommunication)}.
     * 
     * @throws RebarException 
     * @throws InvalidProtocolBufferException 
     * @throws RiakException 
     */
    @Test
    public void testWrite() throws RebarException, InvalidProtocolBufferException, RiakException {
        UUID id = IdUtil.generateUUID();
        String idString = IdUtil.uuidToString(id);
        CommunicationGUID guid = CommunicationGUID.newBuilder()
                .setCorpusName("test")
                .setCommunicationId("1")
                .build();
        KnowledgeGraph graph = KnowledgeGraph.newBuilder()
                .setUuid(IdUtil.generateUUID())
                .build();
        Communication comm = Communication.newBuilder()
                .setUuid(id)
                .setText("hello world")
                .setGuid(guid)
                .setKnowledgeGraph(graph)
                .build();
        rw.write(comm);
        
        IRiakClient irc = RiakFactory.pbcClient();
        Bucket b = irc.fetchBucket("comms").execute();
        IRiakObject iro = b.fetch(idString).execute();

        assertNotNull(iro);
        Communication parsedComm = Communication.parseFrom(iro.getValue());
        String parsedId = IdUtil.uuidToString(parsedComm.getUuid());
        this.fetchedIdsToDelete.add(parsedId);
        logger.info("Got inserted object: " + parsedId);
        
        irc.shutdown();
    }
}

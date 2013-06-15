/**
 * 
 */
package edu.jhu.hlt.rebar.riak.communications;

import java.util.HashSet;
import java.util.Set;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;

import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.riak.CorpusResolver;
import edu.jhu.hlt.rebar.riak.KryoRiakCorpusConverter;
import edu.jhu.hlt.rebar.riak.RiakCommunication;
import edu.jhu.hlt.rebar.riak.RiakConstants;
import edu.jhu.hlt.rebar.riak.RiakCorpus;

/**
 * @author max
 *
 */
public class CommunicationReader {

    private IRiakClient irc;
    private Bucket corpusBucket;
    private Bucket commsBucket;
    
    /**
     * @throws RiakException 
     * 
     */
    public CommunicationReader() throws RiakException {
        this.irc = RiakFactory.pbcClient();
        
        this.corpusBucket = this.irc.fetchBucket(RiakConstants.CORPORA_BUCKET_NAME)
                .execute();
        this.commsBucket = this.irc.fetchBucket(RiakConstants.COMMUNICATIONS_BUCKET_NAME)
                .execute();
    }
    
    public Set<RiakCommunication> fetchAllComms(String nameOfCorpusToFetch) throws RebarException {
        Set<RiakCommunication> rcSet = new HashSet<>();
        
        RiakCorpus rc = new RiakCorpus(nameOfCorpusToFetch);
        try {
            rc = this.corpusBucket.fetch(rc)
                    .withConverter(new KryoRiakCorpusConverter())
                    .withResolver(new CorpusResolver())
                    .execute();
            for (String commId : rc.getCommIdSet()) {
                RiakCommunication rComm = new RiakCommunication(commId);
                rComm = this.commsBucket.fetch(rComm)
                            .withConverter(new CommunicationConverter())
                            .withResolver(new CommunicationResolver())
                            .execute();
                rcSet.add(rComm);
            }
        } catch (UnresolvedConflictException | RiakRetryFailedException | ConversionException e) {
            throw new RebarException(e);
        }
        
        
        return rcSet;
    }
    
    public void close() {
        this.irc.shutdown();
    }

}

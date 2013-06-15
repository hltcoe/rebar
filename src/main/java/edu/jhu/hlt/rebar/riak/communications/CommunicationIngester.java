/**
 * 
 */
package edu.jhu.hlt.rebar.riak.communications;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;

import edu.jhu.hlt.rebar.RebarBackends;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.riak.RiakCommunication;
import edu.jhu.hlt.rebar.riak.RiakConstants;
import edu.jhu.hlt.rebar.riak.RiakCorpus;
import edu.jhu.hlt.rebar.riak.RiakCorpusFactory;

/**
 * @author max
 *
 */
public class CommunicationIngester {
    
    private static final Logger logger = Logger.getLogger(CommunicationIngester.class);
    
    private Set<String> commIdsToIngest;
    private List<RiakCommunication> commCollection;
    private RiakCorpus corpusToIngestTo;
    private IRiakClient irc;
    private Bucket commBucket;
    
    /**
     * @throws RebarException 
     * 
     */
    public CommunicationIngester(Collection<RiakCommunication> commsToIngest, RiakCorpus corpusToIngestTo) 
            throws RebarException {
        this.commCollection = new ArrayList<>(commsToIngest);
        this.corpusToIngestTo = corpusToIngestTo;
        this.commIdsToIngest = new TreeSet<>();
        for (RiakCommunication rc : this.commCollection)
            this.commIdsToIngest.add(rc.getId());
        try {
            this.irc = RiakFactory.pbcClient();
            this.commBucket = this.irc.fetchBucket(RiakConstants.COMMUNICATIONS_BUCKET_NAME)
                    .execute();
        } catch (RiakException e) {
            throw new RebarException(e);
        }
    }
    
    public void close() {
        this.irc.shutdown();
    }
    
    private RiakCommunication ingestRiakCommunication(RiakCommunication rc) throws RebarException {
        try {
            return this.commBucket.store(rc)
                .withConverter(new CommunicationConverter())
                .withResolver(new CommunicationResolver())
                .execute();
        } catch (RiakRetryFailedException | UnresolvedConflictException | ConversionException e) {
            throw new RebarException(e);
        }
    }
    
    private RiakCorpus ingestRiakCorpus() throws RebarException {
        this.corpusToIngestTo.addAllCommIds(commIdsToIngest);
        RiakCorpusFactory rcf = (RiakCorpusFactory) RebarBackends.RIAK.getCorpusFactory("");
        this.corpusToIngestTo = rcf.storeCorpus(this.corpusToIngestTo);
        rcf.close();
        return this.corpusToIngestTo;
    }
    
    public void ingest() throws RebarException {
        for (RiakCommunication rc : this.commCollection)
            this.ingestRiakCommunication(rc);
        this.ingestRiakCorpus();
    }
    
    public void bulkIngest(int numThreadsToUse) throws InterruptedException, RebarException {
        List<Thread> threads = new ArrayList<>(numThreadsToUse);

        int numComms = this.commCollection.size();

        int numCommsPerThread = numComms / numThreadsToUse;
        Iterator<RiakCommunication> commIter = this.commCollection.iterator();
        for (int i = 0; i < numThreadsToUse; i++) {
            int numCommsLeftForThisThread = numCommsPerThread;
            final List<RiakCommunication> threadCollection = new ArrayList<>();
            while (numCommsLeftForThisThread > 0) {
                RiakCommunication rc = commIter.next();
                threadCollection.add(rc);
                numCommsLeftForThisThread--;
            }
            
            Thread t = new Thread() {
                @Override
                public void run() {
                    logger.info("Starting ingest thread...");
                    try {
                        CommunicationIngester ci = new CommunicationIngester(threadCollection, corpusToIngestTo);
                        for (RiakCommunication rc : ci.commCollection)
                            ingestRiakCommunication(rc);
                        ci.close();
                        } catch (RebarException e) {
                            throw new RuntimeException(e);
                        }
                    logger.info("Finished ingesting " + threadCollection.size() + " RiakCommunications.");
                }
            };
            t.start();
            threads.add(t);
        }
        
        logger.info("Main thread ingesting leftover tweets...");
        while(commIter.hasNext())
            ingestRiakCommunication(commIter.next());
        logger.info("Finished.");
        
        for (Thread t : threads)
            t.join();
        ingestRiakCorpus();
        
        logger.info("Finished ingesting all " + this.commCollection.size() + " RiakCommunications.");
    }
    
    public void bulkIngest() throws InterruptedException, RebarException {
        this.bulkIngest(Runtime.getRuntime().availableProcessors());
    }
}

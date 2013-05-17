/**
 * 
 */
package edu.jhu.rebar.riak;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;

import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.CorpusFactory;
import edu.jhu.rebar.RebarException;

/**
 * @author max
 * 
 */
public class RiakCorpusFactory implements CorpusFactory {
    
    private final IRiakClient irc;
    private Bucket bucket;
    
    /**
     * @throws RiakException
     * 
     */
    public RiakCorpusFactory() throws RiakException {
        this.irc = RiakFactory.pbcClient();
        this.bucket = this.irc.fetchBucket(RiakConstants.CORPORA_BUCKET_NAME).execute();
    }
    
    private RiakCorpusFactory(String bucketNamespace) throws RiakException {
        this.irc = RiakFactory.pbcClient();
        this.bucket = this.irc.fetchBucket(bucketNamespace).execute();
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.CorpusFactory#makeCorpus(java.lang.String)
     */
    @Override
    public Corpus makeCorpus(String corpusName) throws RebarException {
        try {
            RiakCorpus rc = new RiakCorpus(corpusName);
            IRiakObject corpusToGet = this.bucket.fetch(corpusName).execute();
            // if null, it does not exist. create it.
            if (corpusToGet == null) {
                rc = this.bucket
                        .store(rc)
                        .withConverter(new KryoRiakCorpusConverter())
                        .returnBody(true)
                        .execute();
                return rc;
            } else {
                // it exists, so let's fetch it.
                return this.bucket.fetch(rc)
                        .withConverter(new KryoRiakCorpusConverter())
                        .execute();
            }
        } catch (UnresolvedConflictException | ConversionException | RiakException re) {
            throw new RebarException(re);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.CorpusFactory#getCorpus(java.lang.String)
     */
    @Override
    public Corpus getCorpus(String corpusName) throws RebarException {
        try {
            IRiakObject corpusToGet = this.bucket.fetch(corpusName).execute();
            // if null, it does not exist. create it.
            if (corpusToGet == null) {
                throw new RebarException("Corpus " + corpusName + " does not exist. Call makeCorpus() instead.");
            } else {
                // it exists, so let's fetch it.
                RiakCorpus rc = new RiakCorpus(corpusName);
                return this.bucket.fetch(rc)
                        .withConverter(new KryoRiakCorpusConverter())
                        .execute();
            }
        } catch (UnresolvedConflictException | ConversionException | RiakException re) {
            throw new RebarException(re);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.CorpusFactory#corpusExists(java.lang.String)
     */
    @Override
    public boolean corpusExists(String corpusName) throws RebarException {
        Collection<String> bucketList = this.listCorpora();
        return bucketList.contains(corpusName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.CorpusFactory#listCorpora()
     */
    @Override
    public Set<String> listCorpora() throws RebarException {
        try {
            Set<String> bucketList = new TreeSet<>();
            for (String k : this.bucket.keys())
                bucketList.add(k);
            return bucketList;
        } catch (RiakException re) {
            throw new RebarException(re);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.CorpusFactory#deleteCorpus(java.lang.String)
     */
    @Override
    public void deleteCorpus(String corpusName) throws RebarException {
        RiakCorpus rc = new RiakCorpus(corpusName);
        try {
            if (this.corpusExists(corpusName))
                this.bucket
                    .delete(rc)
                    .rw(3)
                    .execute();
            // silently do nothing if corpus does not exist.
        } catch (RiakException re) {
            throw new RebarException(re);
        }
    }
    
    /**
     * Store an {@link RiakCorpus} object into Riak.
     * 
     * @param rc
     */
    public RiakCorpus storeCorpus(RiakCorpus rc) throws RebarException {
        CorpusAddCommunicationsMutation cacm = new CorpusAddCommunicationsMutation(rc.getCommIdSet());
        try {
            rc = this.bucket.store(new RiakCorpus(rc.getName()))
                    .withMutator(cacm)
                    .withResolver(new CorpusResolver())
                    .withConverter(new KryoRiakCorpusConverter())
                    .returnBody(true)
                    .execute();
        } catch (RiakRetryFailedException | UnresolvedConflictException | ConversionException e) {
            throw new RebarException(e);
        }
        return rc;
    }
    
    public void close() {
        this.irc.shutdown();
    }
}

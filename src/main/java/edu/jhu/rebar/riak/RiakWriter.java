/**
 * 
 */
package edu.jhu.rebar.riak;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;

import edu.jhu.concrete.Concrete.Communication;
import edu.jhu.concrete.util.IdUtil;
import edu.jhu.rebar.RebarException;

/**
 * @author max
 *
 */
public class RiakWriter {

    private IRiakClient irc;
    private Bucket bucket;
    
    /**
     * @throws RiakException 
     * 
     */
    public RiakWriter() throws RebarException {
        try {
            this.irc = RiakFactory.pbcClient();
            this.bucket = this.irc.fetchBucket("comms").execute();
        } catch (RiakException e) {
            throw new RebarException(e);
        }
    }
    
    public void write(Communication c) throws RebarException {
        String idString = IdUtil.uuidToString(c.getUuid());
        try {
            this.bucket.store(idString, c.toByteArray()).execute();
        } catch (RiakRetryFailedException | UnresolvedConflictException | ConversionException e) {
            throw new RebarException(e);
        }
    }
    
    public void close() throws RebarException {
        this.irc.shutdown();
    }

}

/**
 * 
 */
package edu.jhu.hlt.rebar.riak;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;

import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public class RiakConnector {
    
    private final IRiakClient irc;

    /**
     * Package private ctor - use {@link RiakConnectorFactory} to get an instance outside of the package.
     * 
     * @throws RebarException 
     * 
     */
    RiakConnector() throws RebarException {
        try {
            this.irc = RiakFactory.pbcClient();
        } catch (RiakException re) {
            throw new RebarException(re);
        }
    }
    
    public void close() {
        this.irc.shutdown();
    }
}

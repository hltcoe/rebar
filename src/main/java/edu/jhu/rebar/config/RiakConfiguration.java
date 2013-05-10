/**
 * 
 */
package edu.jhu.rebar.config;

import com.basho.riak.client.raw.pbc.PBClientConfig;

/**
 * @author max
 *
 */
public class RiakConfiguration {

    private static PBClientConfig config;
    
    static {
        config = new PBClientConfig.Builder()
            .build();
    }
    /**
     * 
     */
    public RiakConfiguration() {
        
    }

    public static PBClientConfig getPBClientConfig() {
        return config;
    }
}

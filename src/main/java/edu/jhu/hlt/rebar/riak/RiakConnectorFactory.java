/**
 * 
 */
package edu.jhu.hlt.rebar.riak;

import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 * 
 */
public class RiakConnectorFactory {

    /**
     * 
     */
    private RiakConnectorFactory() {
        // TODO Auto-generated constructor stub
    }

    public static RiakConnector getConnector() throws RebarException {
        return new RiakConnector();
    }

}

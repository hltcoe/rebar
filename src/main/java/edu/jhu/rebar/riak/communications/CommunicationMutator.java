/**
 * 
 */
package edu.jhu.rebar.riak.communications;

import com.basho.riak.client.cap.Mutation;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.rebar.riak.RiakCommunication;

/**
 * @author max
 *
 */
public class CommunicationMutator implements Mutation<RiakCommunication> {

    private RiakCommunication newComm;
    
    /**
     * 
     */
    public CommunicationMutator(RiakCommunication newComm) {
        this.newComm = newComm;
    }

    @Override
    public RiakCommunication apply(RiakCommunication original) {
        Communication originalComm = original.getComm();
        Communication newCommObject = Communication.newBuilder(originalComm)
                .mergeFrom(this.newComm.getComm())
                .build();
        return new RiakCommunication(newCommObject);
    }

}

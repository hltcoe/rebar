/**
 * 
 */
package edu.jhu.rebar.riak.communications;

import java.util.Collection;
import java.util.Iterator;

import com.basho.riak.client.cap.ConflictResolver;

import edu.jhu.concrete.Concrete.Communication;
import edu.jhu.rebar.riak.RiakCommunication;

/**
 * A class used to resolve conflicts between {@link Communication} objects in
 * Riak.
 * 
 * We currently just merge everything (or attempt to).
 * 
 * @author max
 * 
 */
public class CommunicationResolver implements ConflictResolver<RiakCommunication> {

    /**
     * 
     */
    public CommunicationResolver() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public RiakCommunication resolve(Collection<RiakCommunication> siblings) {
        if (siblings.size() > 1) {
            // We have siblings, need to resolve them
            Iterator<RiakCommunication> i = siblings.iterator();
            RiakCommunication rootRiakComm = i.next();
            
            Communication mergedCommObject = rootRiakComm.getComm();
            while (i.hasNext()) {
                mergedCommObject = Communication.newBuilder(mergedCommObject)
                    .mergeFrom(i.next().getComm())
                    .build();
            }

            return new RiakCommunication(mergedCommObject);
        } else if (siblings.size() == 1) {
            // Only one object - just return it
            return siblings.iterator().next();
        } else {
            // No object returned - return null
            return null;
        }
    }

}

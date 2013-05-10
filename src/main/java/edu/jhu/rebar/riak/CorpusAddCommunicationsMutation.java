/**
 * 
 */
package edu.jhu.rebar.riak;

import java.util.Set;

import com.basho.riak.client.cap.Mutation;

/**
 * When we insert an updated corpus, we only need to add the new communication ids
 * into our communication ID set. This class handles that modification. 
 * 
 * @author max
 *
 */
public class CorpusAddCommunicationsMutation implements Mutation<RiakCorpus> {

    private Set<String> newCommIds;
    
    /**
     * 
     */
    public CorpusAddCommunicationsMutation(Set<String> newCommIds) {
        this.newCommIds = newCommIds;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.cap.Mutation#apply(java.lang.Object)
     */
    @Override
    public RiakCorpus apply(RiakCorpus original) {
        original.addAllCommIds(this.newCommIds);
        return original;
    }

}

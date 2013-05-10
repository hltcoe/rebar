/**
 * 
 */
package edu.jhu.rebar.riak;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.basho.riak.client.cap.ConflictResolver;

/**
 * Riak hands us a bunch of {@link Corpus} objects. This class needs to resolve any inconsistencies
 * and return a single {@link Corpus} object. 
 * 
 * @author max
 *
 */
public class CorpusResolver implements ConflictResolver<RiakCorpus> {

    /**
     * 
     */
    public CorpusResolver() {
        // TODO Auto-generated constructor stub
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.cap.ConflictResolver#resolve(java.util.Collection)
     */
    /**
     * Our merging strategy is basic - since we're working with a {@link Set} object,
     * we'll just add all values, and the data structure will ensure we have no
     * duplicates. 
     */
    @Override
    public RiakCorpus resolve(Collection<RiakCorpus> siblings) {
        if (siblings.size() > 1) {
            Iterator<RiakCorpus> corpora = siblings.iterator();
            RiakCorpus resolvedCorpus = corpora.next();
            while (corpora.hasNext()) {
                RiakCorpus nextCorpus = corpora.next();
                resolvedCorpus.addAllCommIds(nextCorpus.getCommIdSet());
            }
            
            return resolvedCorpus;
        } else if (siblings.size() == 1)
            return siblings.iterator().next();
        else
            return null;
    }

}

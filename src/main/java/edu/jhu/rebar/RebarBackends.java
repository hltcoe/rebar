/**
 * 
 */
package edu.jhu.rebar;

import com.basho.riak.client.RiakException;

import edu.jhu.rebar.riak.RiakCorpusFactory;

/**
 * An enumeration of the available rebar backends. 
 * 
 * @author max
 *
 */
public enum RebarBackends {
    RIAK {
        @Override
        public CorpusFactory getCorpusFactory(String... params) throws RebarException {
            try {
                return new RiakCorpusFactory();
            } catch (RiakException e) {
                throw new RebarException(e);
            }
        }
    }, ACCUMULO {
        @Override
        public CorpusFactory getCorpusFactory(String... params) {
            return new AccumuloCorpusFactory();
        }
    }, FILE {
        @Override
        public CorpusFactory getCorpusFactory(String... params) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("This backend is not yet implemented.");
        }
    };
    
    public abstract CorpusFactory getCorpusFactory(String... params) throws RebarException;
}

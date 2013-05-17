/**
 * 
 */
package edu.jhu.rebar;

import com.basho.riak.client.RiakException;

import edu.jhu.rebar.accumulo.AccumuloCorpusFactory;
import edu.jhu.rebar.file.FileCorpusFactory;
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
        public CorpusFactory getCorpusFactory(String... params) throws RebarException {
            return new FileCorpusFactory(params);
        }
    };
    
    public abstract CorpusFactory getCorpusFactory(String... params) throws RebarException;
}

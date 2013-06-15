/**
 * 
 */
package edu.jhu.hlt.rebar;

import com.basho.riak.client.RiakException;

import edu.jhu.hlt.rebar.accumulo.AccumuloCorpusFactory;
import edu.jhu.hlt.rebar.file.FileCorpusFactory;
import edu.jhu.hlt.rebar.riak.RiakCorpusFactory;

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
            return new FileCorpusFactory();
        }
    };
    
    public abstract CorpusFactory getCorpusFactory(String... params) throws RebarException;
}

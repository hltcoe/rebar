/**
 * 
 */
package edu.jhu.hlt.rebar;

import edu.jhu.hlt.rebar.accumulo.AccumuloCorpusFactory;
import edu.jhu.hlt.rebar.file.FileCorpusFactory;

/**
 * An enumeration of the available rebar backends.
 * 
 * @author max
 * 
 */
public enum RebarBackends {
  ACCUMULO {
    @Override
    public CorpusFactory getCorpusFactory(String... params) {
      return new AccumuloCorpusFactory();
    }
  },
  FILE {
    @Override
    public CorpusFactory getCorpusFactory(String... params) throws RebarException {
      return new FileCorpusFactory();
    }
  };

  public abstract CorpusFactory getCorpusFactory(String... params) throws RebarException;
}

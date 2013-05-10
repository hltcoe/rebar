/**
 * 
 */
package edu.jhu.rebar;

import java.util.Collection;

import edu.jhu.rebar.accumulo.AccumuloBackedCorpus;

/**
 * An accumulo-backed implementation of the {@link CorpusFactory} class.
 * 
 * @author max
 *
 */
public class AccumuloCorpusFactory implements CorpusFactory {

    /**
     * 
     */
    public AccumuloCorpusFactory() {
        // TODO Auto-generated constructor stub
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.CorpusFactory#makeCorpus(java.lang.String)
     */
    @Override
    public Corpus makeCorpus(String corpusName) throws RebarException {
        return new AccumuloBackedCorpus(corpusName, true);
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.CorpusFactory#getCorpus(java.lang.String)
     */
    @Override
    public Corpus getCorpus(String corpusName) throws RebarException {
        return new AccumuloBackedCorpus(corpusName, false);
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.CorpusFactory#corpusExists(java.lang.String)
     */
    @Override
    public boolean corpusExists(String corpusName) throws RebarException {
        return AccumuloBackedCorpus.corpusExists(corpusName);
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.CorpusFactory#listCorpora()
     */
    @Override
    public Collection<String> listCorpora() throws RebarException {
        return AccumuloBackedCorpus.listCorpora();
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.CorpusFactory#deleteCorpus(java.lang.String)
     */
    @Override
    public void deleteCorpus(String corpusName) throws RebarException {
        AccumuloBackedCorpus.deleteCorpus(corpusName);
    }
}

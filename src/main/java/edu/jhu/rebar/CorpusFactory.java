/**
 * 
 */
package edu.jhu.rebar;

import java.util.Collection;

/**
 * An interface for creating a "corpus factory", a top level corpus object that
 * can manage corpora in the backend (e.g., corpus creation and deletion).
 * 
 * @author max
 * 
 */
public interface CorpusFactory {
    public Corpus makeCorpus(String corpusName) throws RebarException;

    public Corpus getCorpus(String corpusName) throws RebarException;

    public boolean corpusExists(String corpusName) throws RebarException;

    public Collection<String> listCorpora() throws RebarException;

    public void deleteCorpus(String corpusName) throws RebarException;
}

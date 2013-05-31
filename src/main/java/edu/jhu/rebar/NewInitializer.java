/**
 * 
 */
package edu.jhu.rebar;

import edu.jhu.hlt.concrete.Concrete.Communication;

/**
 * New initializer interface.
 * 
 * @author max
 */
public interface NewInitializer {
    public String getCorpusName();
    public void ingest (Communication comm) throws RebarException;
    public Corpus initialize() throws RebarException;
    public boolean communicationExists(String commId) throws RebarException;
    public void close() throws RebarException;
}

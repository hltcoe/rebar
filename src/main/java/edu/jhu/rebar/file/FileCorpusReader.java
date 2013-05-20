/**
 * 
 */
package edu.jhu.rebar.file;

import java.util.Collection;
import java.util.Iterator;

import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.Corpus.Reader;
import edu.jhu.rebar.IndexedCommunication;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;

/**
 * @author max
 *
 */
public class FileCorpusReader implements Reader {

    /**
     * 
     */
    public FileCorpusReader() {
        // TODO Auto-generated constructor stub
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus.Reader#loadCommunication(java.lang.String)
     */
    @Override
    public IndexedCommunication loadCommunication(String comid) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus.Reader#loadCommunications(java.util.Collection)
     */
    @Override
    public Iterator<IndexedCommunication> loadCommunications(Collection<String> subset) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus.Reader#loadCommunications()
     */
    @Override
    public Iterator<IndexedCommunication> loadCommunications() throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus.Reader#close()
     */
    @Override
    public void close() throws RebarException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus.Reader#getInputStages()
     */
    @Override
    public Collection<Stage> getInputStages() throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus.Reader#getCorpus()
     */
    @Override
    public Corpus getCorpus() {
        // TODO Auto-generated method stub
        return null;
    }

}

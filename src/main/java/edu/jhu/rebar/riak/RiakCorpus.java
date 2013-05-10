/**
 * 
 */
package edu.jhu.rebar.riak;

import java.io.File;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import edu.jhu.concrete.Concrete.Communication;
import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.IndexedCommunication;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;

/**
 * Riak-backed {@link Corpus} implementation.
 * 
 * @author max
 *
 */
public class RiakCorpus extends RiakStagedDataCollection implements Corpus {

    protected Set<String> commIdSet = new TreeSet<>();
    
    public RiakCorpus() {
        super();
    }
    
    public void addCommId(String commId) {
        this.commIdSet.add(commId);
    }
    
    public void addAllCommIds(Collection<String> commIdCollection) {
        this.commIdSet.addAll(commIdCollection);
    }
    
    
    /**
     * @throws RebarException 
     * 
     */
    public RiakCorpus(String name) {
        super(name);
    }
    
    public Set<String> getCommIdSet() {
        return this.commIdSet;
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#readComIdSet(java.io.File)
     */
    @Override
    public Collection<String> readComIdSet(File filename) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#registerComIdSet(java.lang.String, java.util.Collection)
     */
    @Override
    public void registerComIdSet(String name, Collection<String> idSet) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#lookupComIdSet(java.lang.String)
     */
    @Override
    public Collection<String> lookupComIdSet(String name) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#getComIdSetNames()
     */
    @Override
    public Collection<String> getComIdSetNames() throws RebarException {
        // TODO Auto-generated method stub
        //throw new UnsupportedOperationException("Method not yet implemented.");
        return null;
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#makeInitializer()
     */
    @Override
    public Initializer makeInitializer() throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#makeReader(java.util.Collection)
     */
    @Override
    public Reader makeReader(Collection<Stage> stages) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#makeReader(edu.jhu.rebar.Stage[])
     */
    @Override
    public Reader makeReader(Stage[] stages) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#makeReader(edu.jhu.rebar.Stage)
     */
    @Override
    public Reader makeReader(Stage stage) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#makeReader()
     */
    @Override
    public Reader makeReader() throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#makeReader(java.util.Collection, boolean)
     */
    @Override
    public Reader makeReader(Collection<Stage> stages, boolean loadStageOwnership) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#makeReader(edu.jhu.rebar.Stage[], boolean)
     */
    @Override
    public Reader makeReader(Stage[] stages, boolean loadStageOwnership) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#makeReader(edu.jhu.rebar.Stage, boolean)
     */
    @Override
    public Reader makeReader(Stage stage, boolean loadStageOwnership) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#makeReader(boolean)
     */
    @Override
    public Reader makeReader(boolean loadStageOwnership) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#getNumCommunications()
     */
    @Override
    public long getNumCommunications() throws RebarException {
        return this.commIdSet.size();
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#makeWriter(edu.jhu.rebar.Stage)
     */
    @Override
    public Writer makeWriter(Stage stage) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }
    
    private class Writer extends RiakWriter implements Corpus.Writer {
        private final Stage stage;

        private Writer(Stage stage) throws RebarException {
            super();
            this.stage = stage;
        }
        @Override
        public void saveCommunication(IndexedCommunication comm) throws RebarException {
            Communication noIndex = comm.getProto();
            this.write(noIndex);
        }

        @Override
        public void flush() throws RebarException {
            // TODO Auto-generated method stub
            // nothing
        }

        @Override
        public void close() throws RebarException {
            this.close();
        }

        @Override
        public Stage getOutputStage() {
            return this.stage;
        }
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus#makeDiffWriter(edu.jhu.rebar.Stage)
     */
    @Override
    public DiffWriter makeDiffWriter(Stage stage) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

}

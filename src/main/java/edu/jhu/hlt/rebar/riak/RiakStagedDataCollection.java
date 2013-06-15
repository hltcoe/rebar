/**
 * 
 */
package edu.jhu.hlt.rebar.riak;

import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.basho.riak.client.convert.RiakKey;

import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Stage;
import edu.jhu.hlt.rebar.StagedDataCollection;

/**
 * Riak-backed implementation of {@link StagedDataCollection} interface.
 * 
 * @author max
 * 
 */
public abstract class RiakStagedDataCollection implements StagedDataCollection {

    @RiakKey
    protected String name;

    public RiakStagedDataCollection() {
    }
    
    /**
     * 
     */
    public RiakStagedDataCollection(String name) {
        this.name = name;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.StagedDataCollection#getName()
     */
    /**
     * Name getter.
     */
    @Override
    public String getName() {
        return this.name;
    }
    
    /**
     * Name setter.
     * 
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.StagedDataCollection#close()
     */
    @Override
    public void close() throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.StagedDataCollection#makeStage(java.lang.String,
     * java.lang.String, java.util.Collection, java.lang.String, boolean)
     */
    @Override
    public Stage makeStage(String stageName, String stageVersion, Set<Stage> dependencies, String description, boolean deleteIfExists)
            throws RebarException {
//        Stage stage = new RiakStage(stageName, stageVersion, owner, description, stageId, dependencies)
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.jhu.rebar.StagedDataCollection#markStagePublic(edu.jhu.rebar.Stage)
     */
    @Override
    public void markStagePublic(Stage stage) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.StagedDataCollection#getStage(java.lang.String,
     * java.lang.String)
     */
    @Override
    public Stage getStage(String stageName, String stageVersion) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.StagedDataCollection#getStage(int)
     */
    @Override
    public Stage getStage(int stageId) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.StagedDataCollection#getStage(java.lang.String)
     */
    @Override
    public Stage getStage(String stageString) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.StagedDataCollection#getStages()
     */
    @Override
    public SortedSet<Stage> getStages() throws RebarException {
        // TODO Auto-generated method stub
        return new TreeSet<Stage>();
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.StagedDataCollection#hasStage(java.lang.String,
     * java.lang.String)
     */
    @Override
    public boolean hasStage(String stageName, String stageVersion) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.StagedDataCollection#deleteStage(edu.jhu.rebar.Stage)
     */
    @Override
    public void deleteStage(Stage stage) throws RebarException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method not yet implemented.");
    }
}

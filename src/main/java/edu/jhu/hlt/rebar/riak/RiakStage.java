/**
 * 
 */
package edu.jhu.hlt.rebar.riak;

import java.util.Set;

import edu.jhu.hlt.rebar.Stage;

/**
 * @author max
 * 
 */
public class RiakStage implements Stage {

    private final String stageName;
    private final String stageVersion;
    private final Object owner;
    private final String description;
    private boolean isPublic; // not final.

    /**
     * The integer id of this stage (name+version) in the corpus or knowledge
     * graph that it belongs to. This id is unique within the corpus or
     * knowledge graph. Also, a given stage may only depend on stages whose ids
     * are less than that stage's id.
     */
    private final int stageId;

    private final Set<Stage> dependencies;

    /**
     * 
     */
    public RiakStage(String stageName, String stageVersion, Object owner, 
            String description, int stageId, Set<Stage> dependencies) {
        this.stageName = stageName;
        this.stageVersion = stageVersion;
        this.owner = owner;
        this.description = description;
        this.stageId = stageId;
        this.dependencies = dependencies;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Stage o) {
        // TODO Auto-generated method stub
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.Stage#getStageName()
     */
    @Override
    public String getStageName() {
        return this.stageName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.Stage#getStageVersion()
     */
    @Override
    public String getStageVersion() {
        return this.stageVersion;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.Stage#getStageId()
     */
    @Override
    public int getStageId() {
        return this.stageId;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.Stage#isPublic()
     */
    @Override
    public boolean isPublic() {
        return this.isPublic;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.Stage#getDependencies()
     */
    @Override
    public Set<Stage> getDependencies() {
        return this.dependencies;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.jhu.rebar.Stage#getDescription()
     */
    @Override
    public String getDescription() {
        return this.description;
    }
    
    /**
     * 
     * @return owner
     */
    public Object getOwner() {
        return this.owner;
    }

}

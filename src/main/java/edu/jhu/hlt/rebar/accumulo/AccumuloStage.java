/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.hlt.rebar.accumulo;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Stage;

/** Package-private implementation of the stage interface for
 * accumulo-backed tables. */
class AccumuloStage implements Stage
{
	private final String stageName;
	private final String stageVersion;
	private final Object owner;
	private final String description;
	private boolean isPublic; // not final.
	
	/** The integer id of this stage (name+version) in the corpus or
	 * knowledge grpah that it belongs to.  This id is unique within
	 * the corpus or knowledge graph.  Also, a given stage may only
	 * depend on stages whose ids are less than that stage's id. */
	private final int stageId;

	private final Set<Stage> dependencies;

	public AccumuloStage(String stageName, String stageVersion, 
						 Object owner, int stageId, 
						 Collection<Stage> dependencies,
						 String description,
						 boolean isPublic) 
		throws RebarException 
	{
		for (Stage dep: dependencies) {
			if (!(dep instanceof AccumuloStage))
				throw new RebarException("Bad dependency stage type "+
										 "(expected AccumuloStage)");
		}
		this.stageName = stageName;
		this.stageVersion = stageVersion;
		this.owner = owner;
		this.stageId = stageId;
		this.dependencies = Collections.unmodifiableSet(new TreeSet<Stage>(dependencies));
		this.isPublic = isPublic;
		this.description = description;
	}

	public Object getOwner() {
		return owner;
	}

	public boolean isPublic() {
		return this.isPublic;
	}

	/** This should only be called by AccumuloStageTable. */
	protected void markAsPublic() {
		this.isPublic = true;
	}

	//======================================================================
	// Stage interface methods.
	@Override
	public String getStageName() { 
		return stageName; 
	}

	@Override
	public String getStageVersion() { 
		return stageVersion; 
	}

	@Override
	public int getStageId() { 
		return stageId; 
	}

	@Override
	public Set<Stage> getDependencies() { 
		return this.dependencies; 
	}

	@Override
	public String getDescription() { 
		return description; 
	}

	//======================================================================
	// Comparable methods
	@Override
	public int compareTo(Stage o) {
		if (!(o instanceof AccumuloStage))
			throw new RuntimeException("Attempt to compare an AccumuloStage "+
									   "to some other type of stage");
		AccumuloStage ao = (AccumuloStage)o;
		if (owner != ao.owner) 
			throw new RuntimeException("Attempt to compare two Stages that belong "+
									   "to different owners (for ordering)");
		// Note: we assume no overflow here.
		return stageId - ao.stageId;
	}

	//======================================================================
	// Object methods
	@Override
	public String toString() {
		return stageName+":"+stageVersion;
	}

	@Override
	public int hashCode(){
		return Integer.valueOf(stageId).hashCode();
	}

	@Override
	public boolean equals(Object o) {
		return ((o instanceof AccumuloStage) &&
				(stageId == ((AccumuloStage)o).stageId) &&
				(owner == ((AccumuloStage)o).owner));
	}

}

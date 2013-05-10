/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.rebar;

import java.util.Set;

/** A record for a processing stage, which adds information to a REBAR
 * corpus or knowledge graph.  Each stage is uniquely identified
 * (within a given corpus or knowledge graph) by a name string and a
 * version string.  
 *
 * In addition, each processing stage is assigned a unique integer
 * identifier when it is first created (unique within the corpus or
 * knowledge graph that contains the stage).  These are assigned in
 * monotonic order, meaning that if stage Y was created after stage X,
 * then Y's id will be greater than X's id.
 *
 * Finally, each stage contains a list of stages that it depends on
 * (i.e., that should be read before this stage when loading objects).
 * The integer id of any stage is required to be strictly greater than
 * the integer ids of any stages it depends on.  (This ensures that
 * there are no cycles in the dependency graph, and makes it easy to
 * load the stages in the correct order.)
 *
 * Stage objects are immutable.  Equality and ordering comparisons
 * between stages are based on thier integer ids.
 */
public interface Stage extends Comparable<Stage> {

	/** Return the name of this stage. */
	public String getStageName();

	/** Return the version string for this stage. */
	public String getStageVersion();

	/** Return the unique integer id for this stage.  Stage ids are
	 * assigned in monotonic order. */
	public int getStageId();

	/** Return true if this stage has been made "public."  Use
	 * StagedDataCollection::markStagePublic() to make a stage
	 * public. */
	public boolean isPublic();

	/** Return a list of the dependencies of this stage.  The returned
	 * set is sorted (by stage id) and unmodifiable. */
	public Set<Stage> getDependencies();

	/** Return a description of what this stage contains. */
	public String getDescription();

	/** Compares the specified object with this stage for equality.
	 * Two stage objects are considered equal if their integer ids are
	 * the same and they are owned by the same corpus or knowledge
	 * graph. */
	public boolean equals(Object o);
}

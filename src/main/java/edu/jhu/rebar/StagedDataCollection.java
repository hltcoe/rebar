/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.rebar;

import java.util.Collection;
import java.util.SortedSet;

/**
 * Base interface for corpora and graphs.
 */
public interface StagedDataCollection {

    /** Return the name of this corpus. */
    public String getName();

    /** Free any resources used by this corpus. */
    public void close() throws RebarException;

    /** Return the total size of this corpus (in bytes). */
    //public long getSize() throws RebarException;

    // ======================================================================
    // Stages
    // ======================================================================

    /**
     * Create and return a new stage with the given name, version, and list of
     * dependencies. The new stage will be assigned a new stageId that is unique
     * within the corpus and is greater than the stageId of any existing stages.
     * 
     * @param dependencies
     *            The stages that the new stage depends on.
     * @param description
     *            A string description of this stage. This should contain a
     *            human-readable description of exactly what the stage adds.
     * @param deleteIfExists
     *            Controls behavior if a stage with the given name and version
     *            already exists in this collection. If true, then delete the
     *            existing stage before returning. If false, then raise an
     *            exception.
     * @raise RebarException if a stage with the given name and version already
     *        exists (and deleteIfExists=false)
     */
    public Stage makeStage(String stageName, String stageVersion, Collection<Stage> dependencies, String description, boolean deleteIfExists)
            throws RebarException;

    /**
     * Mark the given stage as "public." This should be done only after the
     * stage has been fully written. For a stage to be marked public, all of its
     * dependencies must first be marked public. Once a stage has been marked as
     * public, it can not be made unpublic.
     */
    public void markStagePublic(Stage stage) throws RebarException;

    /**
     * Look up and return the existing stage with the given name and version.
     * 
     * @raise RebarException if no such stage is found.
     */
    public Stage getStage(String stageName, String stageVersion) throws RebarException;

    /**
     * Look up and return the existing stage with the given id.
     * 
     * @raise RebarException if no such stage is found.
     */
    public Stage getStage(int stageId) throws RebarException;

    /**
     * Return the specified stage. If stageString has the form NNN:VVV, then NNN
     * is treated as the name and VVV as the version. Otherwise, stageString is
     * treated as a stage name, and the most recently created *public* stage
     * with the given name is returned.
     * 
     * @raise RebarException if no such stage is found.
     */
    public Stage getStage(String stageString) throws RebarException;

    /**
     * Return the set of all stages that have generated output for this corpus,
     * sorted by their integer ids (i.e., by the order in which they were
     * created).
     */
    public SortedSet<Stage> getStages() throws RebarException;

    /** Return true if a stage with the given name and version exists. */
    public boolean hasStage(String stageName, String stageVersion) throws RebarException;

    /**
     * Delete a specified stage. This will fail (and raise an exception) if any
     * other stage depends on the given stage.
     */
    public void deleteStage(Stage stage) throws RebarException;

}

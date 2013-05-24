/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


/** Interface for reading and writing from REBAR corpora.
 */
package edu.jhu.rebar;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import edu.jhu.hlt.concrete.Concrete;

/**
 * Interface for reading and writing from a "REBAR Corpus".  Each
 * "REBAR Corpus" consists of a set of Communications.
 *
 * Each corpus is backed by some external shared resource (such as an
 * Accumulo table or a directory on a filesystem), and any changes
 * made to the corpus are persistant.
 *
 * Corpus objects are created using the factory class
 * "Corpus.Factory".  In particular, use Corpus.Factory.makeCorpus()
 * to create a new corpus, and Corpus.Factory.getCorpus() to get a
 * connection to an existing corpus.
 */
public interface Corpus extends StagedDataCollection {

    // ======================================================================
    // Identifier Sets
    // ======================================================================

    /** Return a new identifier set containing the identifiers listed
     * in the specified file.  The file should contain one identifier
     * per line. */
    public Collection<String> readComIdSet(File filename) throws RebarException;

    /** Permanently associate a name with a given identifier set.
     * This identifier set can then be looked up using the method
     * 'lookupComIdSet()'. */
    public void registerComIdSet(String name, Collection<String> idSet) throws RebarException;

    /** Look up and return a symbolic identifier set that has been
     * defined using the 'registerComIdSet()' method. */
    public Collection<String> lookupComIdSet(String name) throws RebarException;

    /** Get a list of the names of all symbolic communication
     * identifier sets that have been defined for this corpus. */
    public Collection<String> getComIdSetNames() throws RebarException;

    // ======================================================================
    // Initializer
    // ======================================================================

    /** Interface used to initialize a new corpus by defining the set
     * of communications that it contains.  
     *
     * (This interface could also be used to add new Communications to
     * an existing corpus, but this should be done with care, since
     * any stages that have been run will not contain output for these
     * new Communications.)
     */
    interface Initializer {
	public IndexedCommunication addCommunication(Concrete.Communication comm) throws RebarException;
	public boolean communicationExists(String commId) throws RebarException;
	public void close() throws RebarException;
    }

    /** Return a Corpus.Initializer that can be used to add
     * Communication objects to this corpus. */
    public Initializer makeInitializer() throws RebarException;

    // ======================================================================
    // Reader
    // ======================================================================

    /** Interface for "corpus readers", which are used to read a
     * communications from a corpus, including the output of a
     * specified set of stages. */
    interface Reader {
	/** Read a single communication */
	public IndexedCommunication loadCommunication(String comid) throws RebarException;

	/** Read a specified set of communications.
	 *
	 * @param subset A collection identifying which set of
	 *     communications should be returned by the iterator.
	 **/
	public Iterator<IndexedCommunication> loadCommunications(Collection<String> subset) throws RebarException;

	/** Read all communications */
	public Iterator<IndexedCommunication> loadCommunications() throws RebarException;

	/** Release any resources associated with this reader. */
	public void close() throws RebarException;

	/** Return the list of input stages for this reader. */
	public Collection<Stage> getInputStages() throws RebarException;
		
	/** Return the corpus that owns this reader. */
	Corpus getCorpus();
    }

    /** Return a Corpus.Reader that reads communications from this
     * corpus. 
     *
     * @param stages A list of stages whose output should be included
     *    in the returned Communications.
     */
    public Reader makeReader(Collection<Stage> stages) throws RebarException;
    public Reader makeReader(Stage stages[]) throws RebarException;
    public Reader makeReader(Stage stage) throws RebarException;
    public Reader makeReader() throws RebarException;

    public Reader makeReader(Collection<Stage> stages, boolean loadStageOwnership) throws RebarException;
    public Reader makeReader(Stage stages[], boolean loadStageOwnership) throws RebarException;
    public Reader makeReader(Stage stage, boolean loadStageOwnership) throws RebarException;
    public Reader makeReader(boolean loadStageOwnership) throws RebarException;

    /** Return the number of communications in this Corpus. */
    public long getNumCommunications() throws RebarException;

    // ======================================================================
    // Writer
    // ======================================================================

    /** Interface for "corpus writers," which are used to add
     * the output of a processing stage to some or all of the
     * communications in a corpus.
     */
    interface Writer {
	/** Save any changes that have been made to the given
	 * communication (or do nothing if no changes have been made).
	 * Changes can be made to an IndexedCommunication using its
	 * addField() and SetField() methods, as well as various
	 * convenience methods (such as addTokenization()) that
	 * delegate to these methods. */
	public void saveCommunication(IndexedCommunication comm) throws RebarException;

	public void flush() throws RebarException;
	public void close() throws RebarException;
	public Stage getOutputStage();
    }

    /** Return a Corpus.Writer that can be used to add the output
     * of a processing stage to this corpus. */
    public Writer makeWriter(Stage stage) throws RebarException;

    /** Interface used by RPC to write "diffs", i.e., monotonic
     * changes to communications.  Each diff consists of a dictionary
     * that maps from UUIDs of objects contained within a
     * communication to serialized protobuf messages that should be
     * merged into those objects.  Since diffs are merged in, they
     * can only be used to make monotonic changes (i.e., add field
     * values or set field values, not remove or modify them). */
    interface DiffWriter {
	public void saveCommunicationDiff(String comid, Map<ProtoIndex.ModificationTarget, byte[]> changes) throws RebarException;
	public void flush() throws RebarException;
	public void close() throws RebarException;
    }
    public DiffWriter makeDiffWriter(Stage stage) throws RebarException;

}

/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.rebar.accumulo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner.VarLenEncoder;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.io.Text;

import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;
import edu.jhu.rebar.util.ByteUtil;

/** An interface to a "stage table", which records the set of stages
 * that have been generated for a given corpus or knowledge graph. 
 * 
 * The accumulo table that backs each AccumuloStageTable can contain
 * the following cells:
 *
 * <pre>
 *   RowId    ColFam        ColQual     Value
 * --------- ------------- ---------- -----------------------
 *  stageId   "name"        null       stageName
 *  stageId   "version"     null       stageVer
 *  stageId   "dependency"  depStageId null
 *  stageId   "deleted"     null       null
 *  stageId   "description" null       description
 *  stageId   "isPublic"    null       null
 * </pre>
 */

/*package-private*/ 
class AccumuloStageTable {
	// ======================================================================
	// Constants
	// ======================================================================

	private final static Text STAGE_NAME_CF = new Text("name");
	private final static Text STAGE_VERSION_CF = new Text("version");
	private final static Text STAGE_DEPENDENCY_CF = new Text("dep");
	private final static Text STAGE_IS_PUBLIC_CF = new Text("isPublic");
	private final static Text STAGE_DELETED_CF = new Text("deleted");
	private final static Text STAGE_DESCRIPTION_CF = new Text("description");
	private final static Text STAGE_LOCK_CF = new Text("lock");
	private final static Text EMPTY_CQ = new Text();
	private final static Value EMPTY_VALUE = new Value(new byte[]{});
	private final static Value VALUE_ONE = new Value(new VarLenEncoder().encode(1l));

	// ======================================================================
	// Private Variables
	// ======================================================================

	/** The name of the accumulo table that backs this AccumuloStageTable */
	private final String tableName;
	/** The connection to accumulo */
	private final AccumuloConnector accumuloConnector;
	/** The corpus or graph that owns this stage table. */
	private Object owner;

	/** The stages in this table, indexed by identifier. */
	private SortedMap<Integer, Stage> stagesById = null;
	/** The stages in this table, indexed by name. */
	private Map<String, Map<String, Stage>> stagesByName = null;
	/** When did we last re-read the stage table? */
	private float lastReadTime = 0;

	// ======================================================================
	// Constructor
	// ======================================================================

	AccumuloStageTable(AccumuloConnector accumuloConnector, String tableName, Object owner) {
		this.accumuloConnector = accumuloConnector;
		this.tableName = tableName; // = corpusName+STAGES_TABLE_SUFFIX
		this.owner = owner;
		stagesById = new TreeMap<Integer, Stage>();
		stagesByName = new HashMap<String, Map<String, Stage>>();
	}

	// ======================================================================
	// Public Methods
	// ======================================================================

	public SortedSet<Stage> getStages() throws RebarException {
		readStageTable();
		// Get the set of stages, sorted by id.
		return Collections.unmodifiableSortedSet(new TreeSet<Stage>(stagesById.values()));
	}

	public Stage getStage(String stageName) throws RebarException {
		readStageTable();
		int splitPos = stageName.indexOf(":");
		if (splitPos >= 0) {
			return getStage(stageName.substring(0, splitPos),
							stageName.substring(splitPos+1));
		} else {
			// Iterate through stagesById.
			Stage result = null;
			for (Stage stage: stagesById.values()) {
				if (stage.getStageName().equals(stageName) && stage.isPublic())
					return stage;
			}
		}
		throw new RebarException("No public stage with name "+stageName+" was found.");
	}

	public Stage getStage(int stageId) 
		throws RebarException
	{
		readStageTable();
		Stage stage = stagesById.get(stageId);
		if (stage==null)
			throw new RebarException("Stage not found: "+stageId);
		return stage;
	}

	public Stage getStage(String stageName, String stageVersion) 
		throws RebarException
	{
		readStageTable();
		Map<String, Stage> versionMap = stagesByName.get(stageName);
		if (versionMap==null)
			throw new RebarException("Stage name not found: "+stageName);
		Stage stage = versionMap.get(stageVersion);
		if (stage == null)
			throw new RebarException("Stage version not found: "+stageVersion+" (for "+stageName+")");
		return stage;
	}

	public Stage addStage(String stageName, String stageVersion, 
						  Collection<Stage> dependencies, String description, 
						  boolean deleteIfExists)
		throws RebarException 
	{
		setupCombiner();
		// Make sure we have the latest stage information.
		readStageTable(true);
		// Make sure we don't already have this stage.
		if (hasStage(stageName, stageVersion)) {
			Stage oldStage = getStage(stageName, stageVersion);
			if (deleteIfExists)
				deleteStage(oldStage);
			else
				throw new RebarException("Stage "+oldStage+" already exists "+
										 "(stageId="+oldStage.getStageId()+")");
		}
		// Check that no dependencies have been deleted.
		for (Stage stage: dependencies) {
			if (stagesById.get(stage.getStageId()) == null)
				throw new RebarException("Stage "+stage+" not found -- "+
										 "was it deleted?");
		}
		// Choose a new stage id (and lock it so no one else can use it)
		int stageId = getNewStageId();
		//System.err.println("USING STAGE ID "+stageId);
		// Add information about the new stage to Accumulo.
		BatchWriter writer = accumuloConnector.createBatchWriter(tableName);
		Mutation m = new Mutation(new Text(ByteUtil.fromInt(stageId)));
		m.put(STAGE_NAME_CF, EMPTY_CQ, new Value(stageName.getBytes()));
		m.put(STAGE_VERSION_CF, EMPTY_CQ, new Value(stageVersion.getBytes()));
		m.put(STAGE_DESCRIPTION_CF, EMPTY_CQ, new Value(description.getBytes()));
		for (Stage dep: dependencies)
			m.put(STAGE_DEPENDENCY_CF, new Text(ByteUtil.fromInt(dep.getStageId())), EMPTY_VALUE);
		try {
			writer.addMutation(m);
			writer.flush();
			writer.close();
		} catch (MutationsRejectedException e) {
			throw new RebarException(e);
		}
		// Create the new stage.
		Stage stage = new AccumuloStage(stageName, stageVersion, owner, stageId, 
										dependencies, description, false);
		// Register the stage in our local variables.
		if (!stagesByName.containsKey(stageName))
			stagesByName.put(stageName, new HashMap<String, Stage>());
		stagesByName.get(stageName).put(stageVersion, stage);
		stagesById.put(stageId, stage);
		return stage;
	}

	public boolean hasStage(String stageName, String stageVersion) throws RebarException {
		readStageTable();
		return (stagesByName.containsKey(stageName) && 
				stagesByName.get(stageName).containsKey(stageVersion));
	}
	
	public void deleteStage(Stage stage) throws RebarException {
		// We don't actually remove the stage row (because we want to
		// ensure that its id does not get reused, since that could
		// cause confusion).  So instead, we just add a deleted
		// "marker".  If someone subsequently re-creates a stage with
		// the same name and version strings, then it will be assigned
		// a new stage id.
		BatchWriter writer = accumuloConnector.createBatchWriter(tableName);
		Mutation m = new Mutation(new Text(ByteUtil.fromInt(stage.getStageId())));
		m.put(STAGE_DELETED_CF, EMPTY_CQ, EMPTY_VALUE);
		try {
			writer.addMutation(m);
			writer.flush();
			writer.close();
		} catch (MutationsRejectedException e) {
			throw new RebarException(e);
		}
	}

	public void markStagePublic(Stage stage) throws RebarException {
		// Check that all dependencies are public
		for (Stage dep: stage.getDependencies()) {
			if (!dep.isPublic())
				throw new RebarException("Stage "+stage+" can not be made public -- "+
										 "dependency "+dep+" is not public.");
		}
		BatchWriter writer = accumuloConnector.createBatchWriter(tableName);
		Mutation m = new Mutation(new Text(ByteUtil.fromInt(stage.getStageId())));
		m.put(STAGE_IS_PUBLIC_CF, EMPTY_CQ, EMPTY_VALUE);
		try {
			writer.addMutation(m);
			writer.flush();
			writer.close();
		} catch (MutationsRejectedException e) {
			throw new RebarException(e);
		}
		((AccumuloStage)stage).markAsPublic();
	}

	// ======================================================================
	// Private Helper Methods (Reading the Accumulo Table)
	// ======================================================================

	private int getNewStageId() throws RebarException {
		int newStageId = 1;
		// Scan the table to find the highest used stage id.
		final Scanner scanner = accumuloConnector.createScanner(tableName);
		scanner.setRange(new Range());
		for (Map.Entry<Key, Value> entry: scanner) {
			int stageId = ByteUtil.toInt(entry.getKey().getRow().getBytes());
			newStageId = Math.max(stageId+1, newStageId);
		}
		try {
			final BatchWriter writer = accumuloConnector.createBatchWriter(tableName);
			while (true) {
				// Use the selected stage's "lock" column to try to
				// claim ownership.  In particular, we write a "1" to
				// this column and then read it.  Since this column
				// has a summing iterator on it, at most one client
				// will see a "1" when it reads the column back in.
				// So if we see a 1, then we have successfully locked
				// the column and can claim ownership.  If not, then
				// try the next stageId.
				Text rowId = new Text(ByteUtil.fromInt(newStageId));
				Mutation m = new Mutation(rowId);
				m.put(STAGE_LOCK_CF, EMPTY_CQ, VALUE_ONE);
				writer.addMutation(m);
				writer.flush();
				scanner.setRange(new Range(rowId));
				scanner.fetchColumnFamily(STAGE_LOCK_CF);
				boolean already_locked = false;
				// Note: scanner's iterator should generate exactly one value.
				for (Map.Entry<Key, Value> entry: scanner) {
					if (new VarLenEncoder().decode(entry.getValue().get()) == 1) {
						writer.close();
						return newStageId;
					}
				}
				// Lock conflict -- try the next one.
				newStageId++;
			}
		} catch (MutationsRejectedException e) {
			throw new RebarException(e);
		}
	}

	// Note: priority must be less than 20 (since that's the
	// priority of the verisoning iterator.)
	private static final int COMBINER_PRIORITY = 10;

	private void setupCombiner() throws RebarException {
		if (!accumuloConnector.hasIterator(this.tableName, "lock")) {
			// Add a summing combiner to all columns where family="count"
			IteratorSetting combiner = new IteratorSetting(COMBINER_PRIORITY, "lock", SummingCombiner.class);
			combiner.addOption("columns", STAGE_LOCK_CF.toString());
			combiner.addOption("type", "VARLEN");
			accumuloConnector.attachIterator(this.tableName, combiner);
		}
	}

	private void readStageTable() throws RebarException {
		readStageTable(true);
	}

	private void readStageTable(boolean force) throws RebarException {
		// If we've read the table within the last second, then don't
		// bother to reread it.
		float now = System.currentTimeMillis();
		if (!force && !stagesById.isEmpty() && ((now-lastReadTime)<1000))
			return;
		lastReadTime = now;

		// Read the stage table.
		Scanner scanner = accumuloConnector.createScanner(tableName);
		scanner.setRange(new Range());
		Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
		Text row = null;
		String stageName = null;
		String stageVersion = null;
		List<Integer> stageDeps = new ArrayList<Integer>();
		String description = null;
		boolean isPublic = false;
		boolean deleted = false;
		while (iterator.hasNext()) {
			Map.Entry<Key,Value> cell = iterator.next();
			Key key = cell.getKey();
			if (row == null) {
				row = key.getRow();
			} else if (key.compareRow(row)!=0) {
				int stageId = ByteUtil.toInt(row.getBytes());
				if ((!deleted) && (stageName!=null))
					readStageRow(stageId, stageName, stageVersion, stageDeps, description, isPublic);
				row = key.getRow();
				stageName = null;
				stageVersion = null;
				stageDeps.clear();
				deleted = false;
				isPublic = false;
			}
			if (key.compareColumnFamily(STAGE_NAME_CF)==0) {
				assert(stageName == null);
				stageName = cell.getValue().toString();
			} else if (key.compareColumnFamily(STAGE_VERSION_CF)==0) {
				assert(stageVersion == null);
				stageVersion = cell.getValue().toString();
			} else if (key.compareColumnFamily(STAGE_DEPENDENCY_CF)==0) {
				assert(cell.getValue().equals(EMPTY_VALUE));
				assert(key.getColumnQualifier().getLength()==4);
				stageDeps.add(ByteUtil.toInt(key.getColumnQualifier().getBytes()));
			} else if (key.compareColumnFamily(STAGE_DESCRIPTION_CF)==0) {
				assert(description==null);
				description = cell.getValue().toString();
			} else if (key.compareColumnFamily(STAGE_DELETED_CF)==0) {
				assert(cell.getValue().equals(EMPTY_VALUE));
				deleted = true;
			} else if (key.compareColumnFamily(STAGE_IS_PUBLIC_CF)==0) {
				assert(cell.getValue().equals(EMPTY_VALUE));
				isPublic = true;
			} else if (key.compareColumnFamily(STAGE_LOCK_CF)==0) {
				// nothing to do.
			} else {
				throw new RebarException("While reading stages table: unexpected key "+
										 key.toString());
			}
		}
		if (row != null) {
			int stageId = ByteUtil.toInt(row.getBytes());
			if ((!deleted) && (stageName!=null))
				readStageRow(stageId, stageName, stageVersion, stageDeps, description, isPublic);
		}
	}

	/** Helper method for readStageTable */
	private void readStageRow(int stageId, String stageName, String stageVersion, 
							  List<Integer> stageDepIds, String description, boolean isPublic)
	throws RebarException
	{
		// Sanity checks
		if (stageId<=0)
			throw new RebarException("Bad stage id: "+stageId+"<=0");
		if ((stageName == null) || (stageVersion == null))
			throw new RebarException("NULL stage name or version for: "+stageId);
		if (description == null)
			description = "";
		// Convert dependencies from ints->stages.
		List<Stage> stageDeps = new ArrayList<Stage>(stageDepIds.size());
		for(Integer depId: stageDepIds) {
			if (depId <= 0)
				throw new RebarException("Bad dependency stage id: "+depId+"<=0");
			if (depId >= stageId)
				throw new RebarException("Bad dependency stage id: "+depId+">="+stageId);
			Stage dep = stagesById.get(depId);
			if (dep==null) {
				System.err.println("Stage "+stageId+" ("+stageName+":"+stageVersion+") has invalid dependency "+depId+" -- treating it as deleted");
				continue;
				//throw new RebarException("Dependency stage not found: "+depId);
			}
			stageDeps.add(dep);
		}
		// Check if we've already loaded this stage's info.
		if (stagesById.containsKey(stageId)) {
			Stage oldStage = stagesById.get(stageId);
			if (!oldStage.getStageName().equals(stageName))
				throw new RebarException("Stage name for stage "+stageId+" changed: "+
										 oldStage.getStageName()+"->"+stageName);
			if (!oldStage.getStageVersion().equals(stageVersion))
				throw new RebarException("Stage version for stage "+stageId+" changed: "+
										 oldStage.getStageVersion()+"->"+stageVersion);
			if (!oldStage.getDependencies().equals(new TreeSet<Stage>(stageDeps)))
				throw new RebarException("Dependencies for stage "+stageId+" changed: "+
										 oldStage.getDependencies()+"->"+stageDeps);
			// Check if it became public; if so, then mark it as such
			if (isPublic && !oldStage.isPublic())
				((AccumuloStage)oldStage).markAsPublic();
			return;
		}
		// Make sure the name/version is unique.
		if (stagesByName.containsKey(stageName) && 
			stagesByName.get(stageName).containsKey(stageVersion))
			throw new RebarException("Duplicate stage name: "+stageName+":"+stageVersion+
									 "old stage id="+stagesByName.get(stageName).get(stageVersion).getStageId()+", new stage id="+stageId);
		// Create the stage object
		Stage stage = new AccumuloStage(stageName, stageVersion, owner, stageId, 
										stageDeps, description, isPublic);
		// Register it
		if (!stagesByName.containsKey(stageName))
			stagesByName.put(stageName, new HashMap<String, Stage>());
		stagesByName.get(stageName).put(stageVersion, stage);
		stagesById.put(stageId, stage);
	}

}

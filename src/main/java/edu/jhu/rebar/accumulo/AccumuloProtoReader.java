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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import edu.jhu.hlt.concrete.Concrete;
import edu.jhu.hlt.concrete.util.ByteUtil;
import edu.jhu.hlt.concrete.util.IdUtil;
import edu.jhu.rebar.IndexedProto;
import edu.jhu.rebar.ProtoIndex;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;
import edu.jhu.rebar.StageOwnership;

/** An iterator that reads protobuf objects from the Accumulo table.
 * In particular, for each row, it reads a "root" protobuf object from
 * a fixed column, and then merges in zero or more additional protobuf
 * fields (the output of different processing stage). */
/*package-private*/ 
abstract class AccumuloProtoReader
	<ProtoObj extends Message, ProtoBuilder extends Message.Builder, 
     Identifier, Wrapper extends IndexedProto<ProtoObj>> 
{
	//----------------------------------------------------------------------
	// These are the hooks that subclasses must fill in:
	// (constructor)
	abstract protected ProtoBuilder getRootBuilder(Identifier rootId);
	abstract protected Wrapper wrap(Identifier id, ProtoObj obj, Map<Key,Value> row, StageOwnership stageOwnership) throws RebarException;
	abstract protected Text toRowId(Identifier id);
	abstract protected Identifier toIdentifier(Text rowId);

	//----------------------------------------------------------------------
	// These are the hooks that subclasses may fill in:
	protected void configureScanner(ScannerBase scanner) {}

	//----------------------------------------------------------------------
	// Constants
	//----------------------------------------------------------------------
	private final static byte STAGE_CF_PREFIX = 10;
	private final static Text ROOT_CF = new Text(ByteUtil.fromIntWithPrefix(STAGE_CF_PREFIX,0));
	private final static Text EMPTY_CQ = new Text();

	//----------------------------------------------------------------------
	// Private variables
	//----------------------------------------------------------------------
	/** The stage-ids of all stages that should be merged in.
	 * This includes stages that were explicitly specified as well
	 * as any dependencies of those stages. */
	protected final Map<Integer, Stage> stageIds;

	/** The stages that we're reading from (does not currently include
	 * dependencies of those stages). */
	private final Set<Stage> inputStages;

	private final String tableName;

	private final AccumuloConnector accumuloConnector;

	private final Set<ProtoIterator> activeIterators;

	// This const determines the order in which our server-side
	// iterator gets run; anything above 100 or so should be safe
	// since we're just using a single server-side iterator.
	private static final int WHOLE_ROW_ITERATOR_PRIORITY = 1000;

	//----------------------------------------------------------------------

	public Collection<Stage> getInputStages() {
		return inputStages;
	}
	public void close() {
		for (ProtoIterator iter: activeIterators)
			iter.close();
		activeIterators.clear();
	}

	//----------------------------------------------------------------------
	// Constructor
	AccumuloProtoReader(AccumuloConnector accumuloConnector, String tableName, 
						Collection<Stage> inputStages) throws RebarException {
		this.tableName = tableName;
		this.accumuloConnector = accumuloConnector;
		this.inputStages = Collections.unmodifiableSet(new TreeSet<Stage>(inputStages));
		this.activeIterators = new HashSet<ProtoIterator>();

		// Determine which stages we need to read
		stageIds = new TreeMap<Integer, Stage>();
		stageIds.put(0, null); // root cell.
		for(Stage stage: inputStages)
			addStageDependencies(stage, stageIds);
	}

	/** Add the id of the given stage plus any stages it depends on to `ids`. */
	private void addStageDependencies(Stage stage, Map<Integer, Stage> ids) {
		if (!ids.containsKey(stage.getStageId())) {
			ids.put(stage.getStageId(), stage);
			for (Stage dep: stage.getDependencies())
				addStageDependencies(dep, ids);
		}
	}

	public Iterator<Wrapper> read(Collection<Identifier> subset, boolean recordStageOwnership) throws RebarException {
		ProtoIterator iter = new ProtoIterator(subset, recordStageOwnership);
		activeIterators.add(iter);
		return iter;
	}
	
	public Wrapper read(Identifier id, boolean recordStageOwnership) throws RebarException {
		Wrapper result = readIfExists(id, recordStageOwnership);
		if (result == null)
			throw new RebarException("No element found with id="+id+" found");
		//getStageOwnershipFor(id); // TESTING!!!!
		return result;
	}
	
	public Wrapper readIfExists(Identifier id, boolean recordStageOwnership) throws RebarException {
		Collection<Identifier> subset = new ArrayList<Identifier>(1);
		subset.add(id);
		ProtoIterator iter = new ProtoIterator(subset, recordStageOwnership);
		try {
			if (!iter.hasNext())
				return null;
			return iter.next();
		} finally {
			iter.close();
		}
	}

	
	//----------------------------------------------------------------------
	// Iterator

	private class ProtoIterator implements Iterator<Wrapper> {
		/** The scanner used to read in cells.  Might be a simple scanner
		 * or a batch scanner. */
		private final ScannerBase scanner;
		
		/** The scanner's iterator. */
		private final Iterator<Map.Entry<Key, Value>> iterator;

		/** Has this iterator been closed? */
		boolean closed;

		/** Should we record the stage ownership for each value? */
		boolean recordStageOwnership;
		
		public ProtoIterator(Collection<Identifier> subset, boolean recordStageOwnership) throws RebarException {
			this.scanner = createScanner(subset);
			this.iterator = scanner.iterator();
			this.recordStageOwnership = recordStageOwnership;
			this.closed = false;
		}

		@Override
		public boolean hasNext() { 
			if (iterator.hasNext()) {
				return true;
			} else {
				if (!closed) close(); // auto-close at end of iteration.
				return false;
			}
		}
		@Override
		public void remove() { 
			throw new UnsupportedOperationException("You cannot remove with this iterator."); 
		}
		@Override
		public Wrapper next() {
			final Map.Entry<Key,Value> row = iterator.next();
			try {
				return parseRow(row.getKey(), row.getValue(), recordStageOwnership);
			} catch (RebarException e) {
				throw new RuntimeException(e);
			}
		}

		public void close() {
			if (!closed) { // This is safe to call multiple times.
				if (scanner instanceof BatchScanner) {
					((BatchScanner)scanner).close();
				}
				activeIterators.remove(this);
				closed = true;
			}
		}

		private ScannerBase createScanner(Collection<Identifier> subset) throws RebarException {
			ScannerBase scanner;
			if (subset instanceof AccumuloAllRowIdsCollection) {
				scanner = accumuloConnector.createScanner(tableName);
			} else { 
				BatchScanner bs = accumuloConnector.createBatchScanner(tableName);
				bs.setRanges(rangesForSubset(subset));
				scanner = bs;
			}

			// Fetch the columns for each stage we are interested in.
			// Each stage's output will be in a column whose column
			// family is a fixed prefix plus the stage id number; and
			// whose column qualifier is a UUID.
			for (int stageId: stageIds.keySet()) {
				byte[] cf = ByteUtil.fromIntWithPrefix(STAGE_CF_PREFIX, stageId);
				scanner.fetchColumnFamily(new Text(cf));
			}
			configureScanner(scanner);
			// Use WholeRowIterator to fetch a single row at a time.
			scanner.addScanIterator(new IteratorSetting(WHOLE_ROW_ITERATOR_PRIORITY, 
														"wholeRows", WholeRowIterator.class));
			return scanner;
		}

		private Collection<Range> rangesForSubset(Collection<Identifier> identifiers) {
			Collection<Range> ranges = new ArrayList<Range>(identifiers.size());
			for(Identifier identifier: identifiers)
				ranges.add(new Range(toRowId(identifier)));
			return ranges;
		}
	}

	//----------------------------------------------------------------------
	// Row -> Protobuf Parsing

	Map<Key, Value> decodeRow(Key k, Value v) throws RebarException {

		try {
			return WholeRowIterator.decodeRow(k, v);
		} catch (java.io.IOException e) {
			throw new RebarException(e);
		}
	}

	private class StageOutput {
		public final Stage stage;
		public final byte[] protobufBytes;
		public StageOutput(Stage stage, byte[] protobufBytes) {
			this.stage = stage; this.protobufBytes = protobufBytes; }
	}

	protected ProtoObj getRoot(Identifier rootId, Map<Key,Value> decodedRow) 
		throws RebarException
	{
		ProtoBuilder rootBuilder = getRootBuilder(rootId);
		// In some special cases (notably edges) there will not be any
		// column with cf=ROOT_CF.
		for (Map.Entry<Key,Value> entry: decodedRow.entrySet()) {
			if (entry.getKey().compareColumnFamily(ROOT_CF)==0) {
				assert(entry.getKey().compareColumnQualifier(EMPTY_CQ)==0);
				// Merge in the root object.
				try {
					rootBuilder.mergeFrom(entry.getValue().get());
				} catch (InvalidProtocolBufferException e) {
					throw new RebarException(e);
				}
			}
		}
		// rootBuilder.build() is required to return a ProtoObj.
		@SuppressWarnings("unchecked")
		ProtoObj root = (ProtoObj)(rootBuilder.build());
		return root;
	}

	//private final static Concrete.UUID ZERO_UUID = Concrete.UUID.newBuilder().setHigh(0).setLow(0).build();
	private Wrapper parseRow(Key k, Value v, boolean recordStageOwnership) throws RebarException {
		//System.err.print("ROW ");
		//for (byte b: dr.keySet().iterator().next().getRow().getBytes()) 
		//	System.err.print(" "+b);
		//System.err.println();

		Identifier rootId = toIdentifier(k.getRow());


		Map<Key,Value> decodedRow = decodeRow(k, v);

		ProtoObj root = getRoot(rootId, decodedRow);

		// Allocate stage ownership map (if we're generating it)
		StageOwnership stageOwnership = recordStageOwnership?new StageOwnership():null;

		// If that's all we have, then return it.
		Iterator<Map.Entry<Key,Value>> cellIter = decodedRow.entrySet().iterator();
		Map.Entry<Key,Value> cell = getNextStageCell(cellIter);
		if (cell == null) {
			if (stageOwnership != null) stageOwnership.freeze();
			return wrap(rootId, root, decodedRow, stageOwnership);
		}

		// Index the remaining messages by uuid.
		Map<ProtoIndex.ModificationTarget, List<StageOutput>> stageValues = 
		        new HashMap<ProtoIndex.ModificationTarget, List<StageOutput>>();
		for (; cell != null; cell=getNextStageCell(cellIter)) {
			ProtoIndex.ModificationTarget target = 
				new ProtoIndex.ModificationTarget(cell.getKey().getColumnQualifier().getBytes());
			int stageId = ByteUtil.toIntWithPrefix(STAGE_CF_PREFIX, cell.getKey().getColumnFamily().getBytes());
			Stage stage = stageIds.get(stageId);
			List<StageOutput> values = stageValues.get(target);
			if (values == null) {
				values = new ArrayList<StageOutput>(1);
				stageValues.put(target, values);
			} 
			values.add(new StageOutput(stage, cell.getValue().get()));
		}

		// Merge in stage outputs.
		Message mergedRoot = mergeStages(root, stageValues, stageOwnership, null);
		if (!stageValues.isEmpty()) {
			throw new RebarException(stageValues.size()+" stage output(s) not merged: "
									 +stageValues.keySet());
		}

		@SuppressWarnings("unchecked")
		ProtoObj result = ((mergedRoot==null) ? root : (ProtoObj)(mergedRoot));
		if (stageOwnership != null) stageOwnership.freeze();
		return wrap(rootId, result, decodedRow, stageOwnership);
	}

	/** Return the next cell that contains the output of a stage.
	 * These are identified as cells whose column family begins with
	 * STAGE_CF_PREFIX. */
	private Map.Entry<Key,Value> getNextStageCell(Iterator<Map.Entry<Key,Value>> cellIter) {
		while (cellIter.hasNext()) {
			Map.Entry<Key,Value> cell = cellIter.next();
			Text cf = cell.getKey().getColumnFamily();
			if ((cf.charAt(0)==STAGE_CF_PREFIX) && (!cf.equals(ROOT_CF)))
				return cell;
		}
		return null;
	}

	/** Merge the given "stageOutput" into the builder "target", and
	 * update the list "stageOwnership" to record the stage ownership
	 * of each added field.
	 *
	 * Note: 'target' and 'stageOwnership' are "output" parameters,
	 * and are mutated by this method. */
	private void mergeAndRecordStageOwnership(Message newFields, Stage outputStage, 
											  StageOwnership.FieldValuePointer fvp,
											  Message.Builder target, StageOwnership stageOwnership)
		throws RebarException, InvalidProtocolBufferException
	{
		// Add the fields one at a time, adding a StageOwnershipRecord for each value.
		for(Map.Entry<FieldDescriptor,Object> field: newFields.getAllFields().entrySet()) {
			FieldDescriptor fd = field.getKey();
			if (fd.isRepeated()) {
				// Append field value.
				int origLen = target.getRepeatedFieldCount(fd);
				@SuppressWarnings("unchecked")
				List<Message> children = (List<Message>) field.getValue();
				for (int i=0; i<children.size(); i++) {
					target.addRepeatedField(fd, children.get(i));
					stageOwnership.put(new StageOwnership.FieldValuePointer(fvp, fd, i+origLen),
									   outputStage);
				}
			} else if (target.hasField(fd)) {
				if (fd.getType() == FieldDescriptor.Type.MESSAGE) {
					Message newChild = (Message)(field.getValue());
					if (IdUtil.getUUIDOrNull(newChild) != null)
						throw new RebarException("Stage "+outputStage+" set a non-repeated "+
												 "field with a UUID that already has a value: "+
												 fd.getFullName());
					// Merge field value.
					Message.Builder childTarget = ((Message)(target.getField(fd))).toBuilder();
					mergeAndRecordStageOwnership(newChild, outputStage,
												 new StageOwnership.FieldValuePointer(fvp, fd, 0),
												 childTarget, stageOwnership);
					target.setField(fd, childTarget.build());
				} else {
					throw new RebarException("Stage "+outputStage+" set a non-repeated field "+
											 "that already has a value: "+fd.getFullName());
				}
			} else {
				target.setField(fd, field.getValue());
				stageOwnership.put(new StageOwnership.FieldValuePointer(fvp, fd, 0), outputStage);
			}
		}
	}



	/** Returns null if nothing changed, or the new value if we
	 * merged something in. */
	private Message mergeStages(Message msg, Map<ProtoIndex.ModificationTarget, List<StageOutput>> stageValues,
								StageOwnership stageOwnership, StageOwnership.FieldValuePointer fvp)
		throws RebarException
	{
		Message.Builder builder = null;

		// Get the modification target for this message.
		final ProtoIndex.ModificationTarget target;
		Concrete.UUID uuid = IdUtil.getUUIDOrNull(msg); // uuid may be null.
		if (uuid != null)
			target = new ProtoIndex.ModificationTarget(uuid);
		else if (msg instanceof Concrete.Edge) {
			target = new ProtoIndex.ModificationTarget(((Concrete.Edge)msg).getEdgeId());
		}
		else
			target = null;
		//System.err.println("Merging, target="+target);
		// Get any values we should merge into this target, and merge them.
		List<StageOutput> valuesToMerge = stageValues.remove(target);
		if (valuesToMerge != null) {
			try {
				// Merge in the new values.
				for (StageOutput stageOutput: valuesToMerge) {
					if (builder==null) builder = msg.toBuilder();
					if (stageOwnership == null) {
						builder.mergeFrom(stageOutput.protobufBytes);
					} else {
						Message newFields = msg.newBuilderForType()
							.mergeFrom(stageOutput.protobufBytes).buildPartial();
						mergeAndRecordStageOwnership(newFields, stageOutput.stage, 
													 fvp, builder, stageOwnership);
					}
				}
			} catch (InvalidProtocolBufferException e) {
				throw new RebarException(e);
			}
			// If we're done with everything, then return immediately.
			if (stageValues.isEmpty()) return builder.build();
			// If the stage we just merged in is depended on by other
			// stages, then we need to rebuild the message before we
			// traverse it...  Currently we don't keep that info, so
			// for now, just always do it.
			if (true) {
				msg = builder.build();
				builder = null;
			}
		}

		// Recurse to subfields
		for(Map.Entry<FieldDescriptor,Object> field: msg.getAllFields().entrySet()) {
			FieldDescriptor fd = field.getKey();
			if (fd.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
				if (fd.isRepeated()) {
					@SuppressWarnings("unchecked")
					List<Message> children = (List<Message>) field.getValue();
					for (int i=0; i<children.size(); i++) {
						Message child = children.get(i);
						StageOwnership.FieldValuePointer childFvp = 
							(stageOwnership==null)?null:new StageOwnership.FieldValuePointer(fvp, fd, i);
						Message mergedChild = mergeStages(child, stageValues, stageOwnership, childFvp);
						if (mergedChild != null) {
							if (builder==null) builder = msg.toBuilder();
							builder.setRepeatedField(fd, i, mergedChild);
							if (stageValues.isEmpty()) return builder.build();
						}
					}
				} else {
					Message child = (Message) field.getValue();
					StageOwnership.FieldValuePointer childFvp = 
						(stageOwnership==null)?null:new StageOwnership.FieldValuePointer(fvp, fd, 0);
					Message mergedChild = mergeStages(child, stageValues, stageOwnership, childFvp);
					if (mergedChild != null) {
						if (builder==null) builder = msg.toBuilder();
						builder.setField(fd, mergedChild);
						if (stageValues.isEmpty()) return builder.build();
					}
				}
			}
		}

		if (builder != null)
			return builder.build();
		else if (valuesToMerge != null)
			return msg;
		else
			return null;
	}

	//----------------------------------------------------------------------
	// Stage Inspection



}

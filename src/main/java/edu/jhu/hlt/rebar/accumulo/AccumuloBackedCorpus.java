/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.hlt.rebar.accumulo;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import edu.jhu.hlt.concrete.Concrete;
import edu.jhu.hlt.concrete.util.ByteUtil;
import edu.jhu.hlt.rebar.Corpus;
import edu.jhu.hlt.rebar.IndexedCommunication;
import edu.jhu.hlt.rebar.IndexedKnowledgeGraph;
import edu.jhu.hlt.rebar.ProtoIndex;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Stage;
import edu.jhu.hlt.rebar.StageOwnership;
import edu.jhu.hlt.rebar.util.FileUtil;


/**
 * Accumulo-backed implementation of the Corpus interface.  This class
 * should not be directly instantiated; use Corpus.Factory.getCorpus()
 * instead. 
 */
// key.cf = stageid; key.cq = uuid
public class AccumuloBackedCorpus extends AccumuloBackedStagedDataCollection implements Corpus {
    // ======================================================================
    // Constants
    // ======================================================================

    // Table prefix/suffixes
    private final static String TABLE_PREFIX = "corpus_";
    private final static String IDSETS_TABLE_SUFFIX = "_idsets";
    private final static String COMS_TABLE_SUFFIX = "_coms";
    private final static String[] TABLE_SUFFIXES = {COMS_TABLE_SUFFIX, STAGES_TABLE_SUFFIX, IDSETS_TABLE_SUFFIX};
    private final static String[] PROTO_TABLE_SUFFIXES = {COMS_TABLE_SUFFIX};
    private final static byte[] STAGE_CF_PREFIXES = {STAGE_CF_PREFIX};

    // Other constants:
    private final static Text ROOT_CF = new Text(ByteUtil.fromIntWithPrefix(STAGE_CF_PREFIX,0));
    private final static Text EMPTY_CQ = new Text();
	
    private static final Logger LOGGER = Logger.getLogger(AccumuloBackedCorpus.class);

    private static int DEBUG_LEVEL = 0;

    // ======================================================================
    // Private Variables
    // ======================================================================
    private final ComIdSetTable comIdSetTable;
    private final String comsTableName;

    // ======================================================================
    // Constructor
    // ======================================================================
    public AccumuloBackedCorpus(String corpusName, boolean createCorpus) throws RebarException {
	super(corpusName, createCorpus, TABLE_PREFIX, TABLE_SUFFIXES, PROTO_TABLE_SUFFIXES, STAGE_CF_PREFIXES);
	this.comsTableName = TABLE_PREFIX+corpusName+COMS_TABLE_SUFFIX;
	this.comIdSetTable = new ComIdSetTable(accumuloConnector, 
					       TABLE_PREFIX+corpusName+IDSETS_TABLE_SUFFIX);
    }

    // ======================================================================
    // Corpus Interface Methods
    // ======================================================================

    @Override
	public long getNumCommunications() throws RebarException {
	return summaryTable.getCount(getName(), "com");
    }

    @Override
	public Initializer makeInitializer() 
	throws RebarException 
    {
	return new Initializer();
    }

    @Override
	public Reader makeReader(Collection<Stage> stages) 
	throws RebarException
    {
	return new Reader(stages, false);
    }

    @Override
	public Reader makeReader(Stage stages[]) throws RebarException {
	return makeReader(Arrays.asList(stages));
    }

    @Override
	public Reader makeReader(Stage stage) throws RebarException {
	return makeReader(Arrays.asList(new Stage[] {stage}));
    }

    @Override
	public Reader makeReader() throws RebarException {
	return makeReader(new Stage[] {});
    }

    @Override
	public Reader makeReader(Collection<Stage> stages, boolean loadStageOwnership) 
	throws RebarException
    {
	return new Reader(stages, loadStageOwnership);
    }

    @Override
	public Reader makeReader(Stage stages[], boolean loadStageOwnership) throws RebarException {
	return makeReader(Arrays.asList(stages), loadStageOwnership);
    }

    @Override
	public Reader makeReader(Stage stage, boolean loadStageOwnership) throws RebarException {
	return makeReader(Arrays.asList(new Stage[] {stage}), loadStageOwnership);
    }

    @Override
	public Reader makeReader(boolean loadStageOwnership) throws RebarException {
	return makeReader(new Stage[] {}, loadStageOwnership);
    }

    @Override
	public Writer makeWriter(Stage stage) 
	throws RebarException
    {
	return new Writer(stage);
    }

    @Override
	public DiffWriter makeDiffWriter(Stage stage)
	throws RebarException
    {
	return new DiffWriter(stage);
    }

    // ======================================================================
    // Reader
    // ======================================================================

    /** Given a Communication whose Knowledge Graph may contain
     * multiple Edge objects with the same edge_id, merge the
     * attributes of those edges, and remove any duplicates from the
     * vertex neighbor lists.  This is used when loading a
     * communication because we might be loading multiple stages that
     * independently added information for the same edge, in which
     * case they would each have created an Edge object under the
     * knowledge graph; but these will not get automatically merged. */
	
    private Concrete.Communication mergeDuplicateEdges(Concrete.Communication com) {
	Concrete.KnowledgeGraph graph = com.getKnowledgeGraph();
	List<Concrete.Edge> edges = graph.getEdgeList();
	if (edges.size() == 0) return com;
	Map<Concrete.EdgeId, Concrete.Edge> edgeMap = new HashMap<Concrete.EdgeId, Concrete.Edge>();
	Map<Concrete.UUID, Set<Concrete.UUID>> neighbors = new HashMap<Concrete.UUID, Set<Concrete.UUID>>();
	boolean mergeIsNeeded = false;
	for (Concrete.Edge edge: edges) {
	    Concrete.EdgeId edgeId = edge.getEdgeId();
	    // Update edgeMap to include this edge.
	    if (edgeMap.containsKey(edgeId)) {
		Concrete.Edge newEdge = edgeMap.get(edgeId).toBuilder()
		    .mergeFrom(edge).build();
		edgeMap.put(edgeId, newEdge);
		mergeIsNeeded = true;
	    } else {
		edgeMap.put(edgeId, edge);
	    }
	    // Update neighbors map
	    Concrete.UUID v1 = edge.getEdgeId().getV1();
	    Concrete.UUID v2 = edge.getEdgeId().getV2();
	    if (!neighbors.containsKey(v1))
		neighbors.put(v1, new HashSet<Concrete.UUID>());
	    if (!neighbors.containsKey(v2))
		neighbors.put(v2, new HashSet<Concrete.UUID>());
	    neighbors.get(v1).add(v2);
	    neighbors.get(v2).add(v1);
	}
	/*if (!mergeIsNeeded) return com; */
	// Replace the edges/neighbors values.
	Concrete.Communication.Builder comBuilder = com.toBuilder();
	Concrete.KnowledgeGraph.Builder graphBuilder = comBuilder.getKnowledgeGraphBuilder();
	graphBuilder.clearEdge();
	graphBuilder.addAllEdge(edgeMap.values());
	for (Concrete.Vertex.Builder vbuilder: graphBuilder.getVertexBuilderList()) {
	    vbuilder.clearNeighbor();
	    vbuilder.addAllNeighbor(neighbors.get(vbuilder.getUuid()));
	}
	return comBuilder.build();
    }

    /** Private subclass of AccumuloProtoReader that is used to read
     * communications from protobuf. */
    private class ComReader 
	extends AccumuloProtoReader<Concrete.Communication, Concrete.Communication.Builder, String, IndexedCommunication> 
    {
	ComReader(Collection<Stage> stages) throws RebarException {
	    super(accumuloConnector, comsTableName, stages); }
	protected Concrete.Communication.Builder getRootBuilder(String comId) {
	    return Concrete.Communication.newBuilder(); }
	protected IndexedCommunication wrap(String comId, Concrete.Communication comm, Map<Key,Value> row, StageOwnership stageOwnership) 
	    throws RebarException
	{
	    comm = mergeDuplicateEdges(comm);
	    return new IndexedCommunication(comm, new ProtoIndex(comm), stageOwnership); }
	protected Text toRowId(String identifier) {
	    return new Text(identifier); }
	protected String toIdentifier(Text rowId) {
	    return rowId.toString(); }
    }

    /** Corpus.Reader implementation: just a thin wrapper around a ComReader */
    private class Reader implements Corpus.Reader {
	private final ComReader comReader;
	private final boolean loadStageOwnership;
	Reader(Collection<Stage> stages, boolean loadStageOwnership) throws RebarException {
	    this.comReader = new ComReader(stages); 
	    this.loadStageOwnership = loadStageOwnership;
	}

	@Override
	    public IndexedCommunication loadCommunication(String comid) throws RebarException {
	    return comReader.read(comid, loadStageOwnership); }
	@Override
	    public Iterator<IndexedCommunication> loadCommunications(Collection<String> comIds) throws RebarException {
	    return comReader.read(comIds, loadStageOwnership); }
	@Override
	    public Iterator<IndexedCommunication> loadCommunications() throws RebarException {
	    return comReader.read(lookupComIdSet("all"), loadStageOwnership); }
	@Override
	    public void close() throws RebarException {
	    comReader.close(); }
	@Override
	    public Collection<Stage> getInputStages() throws RebarException {
	    return comReader.getInputStages(); }
	@Override
	    public Corpus getCorpus() { return AccumuloBackedCorpus.this; }
    }

    // ======================================================================
    // Initializer
    // ======================================================================

    private class Initializer 
	extends AccumuloWriter 
	implements Corpus.Initializer 
    {
	Initializer() throws RebarException { 
	    super(accumuloConnector, comsTableName); 
	}
		
	@Override
	    public boolean communicationExists(String rowId) throws RebarException {
	    return this.rowExists(new Text(rowId));
	}
		
	@Override
	    public IndexedCommunication addCommunication(Concrete.Communication communication) 
	    throws RebarException 
	{
	    return addCommunication(communication, true);
	}
		
	public IndexedCommunication addCommunication(Concrete.Communication communication, boolean flush) 
	    throws RebarException 
	{
	    Text rowId = rowIdFor(communication);
	    LOGGER.debug("Row id: " + rowId);
	    if (rowExists(rowId))
		throw new RebarException("Duplicate communication!");
	    // Build the indexed communication (this will be our return value).
	    IndexedCommunication com = new IndexedCommunication(communication, new ProtoIndex(communication), null);
	    // Make sure that v1<=v2 for all edges in this communication's 
	    // graph; and that the neighbor lists are correct.
	    IndexedKnowledgeGraph graph = com.getKnowledgeGraph();
	    graph.checkEdgeIds();
	    graph.checkVertexNeighbors();
	    // Write the communication.
	    write(rowId, 
		  AccumuloBackedCorpus.ROOT_CF,
		  AccumuloBackedCorpus.EMPTY_CQ,
		  new Value(communication.toByteArray()));
	    summaryTable.incrementCount(getName(), "com");
	    return com;
	}
    }

    // ======================================================================
    // Writer
    // ======================================================================
	
    private class Writer 
	extends AccumuloWriter 
	implements Corpus.Writer 
    { 
	private final Stage stage;

	private Writer(Stage stage) throws RebarException { 
	    super(accumuloConnector, comsTableName);
	    this.stage = stage;
	}

	@Override
	    public Stage getOutputStage() { return stage; }

	@Override
	    public void saveCommunication(IndexedCommunication com) 
	    throws RebarException
	{
	    Text comId = rowIdFor(com.getProto());
	    if (DEBUG_LEVEL>0 && (!rowExists(comId)))
		throw new RebarException("Communication "+comId+" not found in corpus");
	    // Make sure that v1<=v2 for all edges in this communication's 
	    // graph; and that the neighbor lists are correct.
	    IndexedKnowledgeGraph graph = com.getKnowledgeGraph();
	    graph.checkEdgeIds();
	    graph.checkVertexNeighbors();
	    // Write the changes.
	    writeProtoModifications(stage, comId, com.getIndex());
	}
    }

    // ======================================================================
    // DiffWriter
    // ======================================================================
	
    private class DiffWriter 
	extends AccumuloWriter 
	implements Corpus.DiffWriter 
    { 
	private final Stage stage;

	public DiffWriter(Stage stage) throws RebarException { 
	    super(accumuloConnector, comsTableName);
	    this.stage = stage;
	}

	@Override
	    public void saveCommunicationDiff(String comId, Map<ProtoIndex.ModificationTarget, byte[]> changes) throws RebarException {
	    if (DEBUG_LEVEL>0 && (!rowExists(new Text(comId))))
		throw new RebarException("Communication "+comId+" not found in corpus");
	    // Should we do additional sanity checks on the graph here?
	    writeProtoModifications(stage, new Text(comId), changes);
	}
    }

    // ======================================================================
    // Identifier Sets
    // ======================================================================

    private class ComIdSetTable extends AccumuloIdSetTable<String> {
	ComIdSetTable(AccumuloConnector accumuloConnector, String tableName) {
	    super(accumuloConnector, tableName); }
	protected Text idToRowId(String id) { return new Text(id); }
	protected String rowIdToId(Text id) { return id.toString(); }
    }

    private static class AllComRowIds extends AccumuloAllRowIdsCollection<String> {
	AllComRowIds(AccumuloConnector accumuloConnector, String tableName) {
	    super(accumuloConnector, tableName); }
	protected String rowIdToId(Text row) { 
	    return row.toString(); }
    }

    public Collection<String> readComIdSet(File filename) throws RebarException {
	return FileUtil.getStringListFromFile(filename);
    }

    public void registerComIdSet(String name, Collection<String> idSet) throws RebarException {
	if (name.equalsIgnoreCase("all"))
	    throw new RebarException("The name 'all' is reserved and can not be redefined.");
	comIdSetTable.registerIdSet(name, idSet);
    }

    public Collection<String> lookupComIdSet(String name) throws RebarException {
	if (name.equalsIgnoreCase("all")) {
	    return new AllComRowIds(accumuloConnector, comsTableName);
	} else {
	    return comIdSetTable.lookupIdSet(name);
	}
    }

    public Collection<String> getComIdSetNames() throws RebarException {
	return comIdSetTable.getSubsetNames();
    }

    // ======================================================================
    // Helper Methods
    // ======================================================================
	
    private static Text rowIdFor(Concrete.Communication comm) throws RebarException {
	Text rowId = new Text(comm.getGuid().getCommunicationId());
	if (rowId.getLength()==0)
	    throw new RebarException("Attempt to write a communication with an empty "+
				     "guid.communicationId");
	return rowId;
    }

    // ======================================================================
    // Static Methods
    // ======================================================================
    public static boolean corpusExists(String corpusName) throws RebarException {
	AccumuloConnector accumuloConnector = new AccumuloConnector();
	boolean result = true;
	for (String suffix: TABLE_SUFFIXES) {
	    if (!accumuloConnector.tableExists(TABLE_PREFIX+corpusName+suffix))
		result = false;
	}
	accumuloConnector.release();
	return result;
    }

    public static Set<String> listCorpora() throws RebarException {
	AccumuloConnector accumuloConnector = new AccumuloConnector();
	Map<String, Integer> corpusTableCounts = new HashMap<String, Integer>();
	for (String tableName: accumuloConnector.listTables()) {
	    if (tableName.startsWith(TABLE_PREFIX)) {
		for (String suffix: TABLE_SUFFIXES) {
		    if (tableName.endsWith(suffix)) {
			int s = TABLE_PREFIX.length();
			int e = tableName.length()-suffix.length();
			String corpusName = tableName.substring(s, e);
			Integer oldVal = corpusTableCounts.get(corpusName);
			if (oldVal==null) oldVal = 0;
			corpusTableCounts.put(corpusName, oldVal+1);
		    }
		}
	    }
	}
	Set<String> result = new TreeSet<String>();
	for (Map.Entry<String,Integer> entry: corpusTableCounts.entrySet()) {
	    if (entry.getValue() == TABLE_SUFFIXES.length) {
		result.add(entry.getKey());
	    } else {
		System.err.println("Warning: one or more tables appears to be "+
				   "missing for corpus "+entry.getKey());
	    }
	}
	accumuloConnector.release();
	return result;
    }

    public static void deleteCorpus(String corpusName) throws RebarException {
	AccumuloConnector accumuloConnector = new AccumuloConnector();
	for (String suffix: TABLE_SUFFIXES)
	    accumuloConnector.deleteTable(TABLE_PREFIX+corpusName+suffix);
	AccumuloSummaryTable summaryTable = new AccumuloSummaryTable(accumuloConnector, TABLE_PREFIX);
	summaryTable.deleteEntry(corpusName);
	accumuloConnector.release();
    }

}

/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.rebar.accumulo;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import edu.jhu.rebar.Graph;
import edu.jhu.rebar.IndexedEdge;
import edu.jhu.rebar.IndexedVertex;
import edu.jhu.rebar.ProtoIndex;
import edu.jhu.concrete.Concrete;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;
import edu.jhu.rebar.StageOwnership;
import edu.jhu.rebar.util.ByteUtil;
import edu.jhu.rebar.util.RebarIdUtil;


/*

[VERTEX_ID][NEIGHBOR+stage_id][uuid]

 */


 // XX STAGE DELETION DOES NOT CLEAR NEIGHBOR EDGES???
/**
 * Accumulo-backed implementation of the Graph interface.  This class
 * should not be directly instantiated; use Graph.Factory.getGraph()
 * instead. 
 * 
 * The accumulo table that backs each Graph can contain the following
 * cells:
 *
 * <pre>
 *   RowId    ColFam        ColQual          Value
 * --------- ------------- -------------- -----------------------
 *  edgeId    S+stage       target_uuid       protobuf
 *  edgeId    N+stage       nbr_uuid
 * </pre>
 */
// key.cf = stageid; key.cq = uuid
public class AccumuloBackedGraph extends AccumuloBackedStagedDataCollection implements Graph {
	// ======================================================================
	// Constants
	// ======================================================================

	// We will use a one-byte prefix for each column family type...
	public final static byte STAGE_CF_PREFIX = 10;
	public final static byte NEIGHBOR_CF_PREFIX = 20;

	// Table prefix/suffixes
	private final static String TABLE_PREFIX = "graph_";
	private final static String STAGES_TABLE_SUFFIX = "_stages";
	private final static String VERTEX_TABLE_SUFFIX = "_vertices";
	private final static String EDGE_TABLE_SUFFIX = "_edges";
	private final static String VERTEX_IDSETS_TABLE_SUFFIX = "_vertex_idsets";
	private final static String EDGE_IDSETS_TABLE_SUFFIX = "_edge_idsets";
	private final static String[] TABLE_SUFFIXES = {VERTEX_TABLE_SUFFIX, EDGE_TABLE_SUFFIX,
													STAGES_TABLE_SUFFIX, 
													VERTEX_IDSETS_TABLE_SUFFIX,
													EDGE_IDSETS_TABLE_SUFFIX};
	private final static String[] PROTO_TABLE_SUFFIXES = {VERTEX_TABLE_SUFFIX, EDGE_TABLE_SUFFIX};
	private final static byte[] STAGE_CF_PREFIXES = {STAGE_CF_PREFIX, NEIGHBOR_CF_PREFIX};

	// Other constants:
	public final static Text ROOT_CF = new Text(ByteUtil.fromIntWithPrefix(STAGE_CF_PREFIX,0));
	public final static Text EMPTY_CQ = new Text();
	private final static Concrete.UUID ZERO_UUID = Concrete.UUID.newBuilder().setHigh(0).setLow(0).build();
	private final static Value EMPTY_VALUE = new Value(new byte[]{});

	private static int DEBUG_LEVEL = 0;

	// ======================================================================
	// Private Variables
	// ======================================================================
	private final VertexIdSetTable vertexIdSetTable;
	private final EdgeIdSetTable edgeIdSetTable;
	private final String verticesTableName;
	private final String edgesTableName;

	// ======================================================================
	// Constructor
	// ======================================================================
	public AccumuloBackedGraph(String graphName, boolean createGraph) throws RebarException {
		super(graphName, createGraph, TABLE_PREFIX, TABLE_SUFFIXES, PROTO_TABLE_SUFFIXES, STAGE_CF_PREFIXES);
		this.verticesTableName = TABLE_PREFIX+graphName+VERTEX_TABLE_SUFFIX;
		this.edgesTableName = TABLE_PREFIX+graphName+EDGE_TABLE_SUFFIX;
		this.vertexIdSetTable = new VertexIdSetTable(accumuloConnector, TABLE_PREFIX+graphName+VERTEX_IDSETS_TABLE_SUFFIX);
		this.edgeIdSetTable = new EdgeIdSetTable(accumuloConnector, TABLE_PREFIX+graphName+EDGE_IDSETS_TABLE_SUFFIX);
	}

	// ======================================================================
	// Graph Interface Methods
	// ======================================================================

	@Override
	public long getNumVertices() throws RebarException {
		return summaryTable.getCount(getName(), "vertex");
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
	public Reader makeReader(Collection<Stage> stages, boolean loadStageOwnership) 
		throws RebarException
	{
		return new Reader(stages, loadStageOwnership);
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

	/** Private subclass of AccumuloProtoReader that is used to read
	 * vertices from protobuf. */
	private class VertexReader 
		extends AccumuloProtoReader<Concrete.Vertex, Concrete.Vertex.Builder, Concrete.UUID, IndexedVertex> 
	{
		private final Graph.Reader graphReader;
		VertexReader(Collection<Stage> stages, Graph.Reader graphReader) throws RebarException {
			super(accumuloConnector, verticesTableName, stages); 
			this.graphReader = graphReader;
		}
		protected Concrete.Vertex.Builder getRootBuilder(Concrete.UUID vertexId) {
			return Concrete.Vertex.newBuilder(); 
		}
		protected void configureScanner(ScannerBase scanner) {
            for (int stageId: stageIds.keySet()) {
                byte[] cf = ByteUtil.fromIntWithPrefix(NEIGHBOR_CF_PREFIX, stageId);
                scanner.fetchColumnFamily(new Text(cf));
            }
		}
		protected Text toRowId(Concrete.UUID id) {
			return vertexIdToRowId(id); 
		}
		protected Concrete.UUID toIdentifier(Text rowId) { 
			return rowIdToVertexId(rowId); 
		}
		protected IndexedVertex wrap(Concrete.UUID uuid, Concrete.Vertex vertex, Map<Key,Value> row, StageOwnership stageOwnership) 
			throws RebarException
		{
			// Add in the list of neighbors.
			Concrete.Vertex.Builder vertexBuilder = vertex.toBuilder();
			for (Map.Entry<Key,Value> cell: row.entrySet()) {
				Key key = cell.getKey();
				if (cell.getKey().getColumnFamily().charAt(0)==NEIGHBOR_CF_PREFIX) {
					Concrete.UUID nbr = ByteUtil.toUUID(cell.getKey().getColumnQualifier().getBytes());
					vertexBuilder.addNeighbor(nbr);
				}
			}
			Concrete.Vertex vertexWithNeighbors = vertexBuilder.build();
			ProtoIndex protoIndex = new ProtoIndex(vertexWithNeighbors);
			return new IndexedVertex(vertexWithNeighbors, protoIndex, stageOwnership, graphReader); 
		}
	}

	/** Private subclass of AccumuloProtoReader that is used to read
	 * edges from protobuf. */
	private class EdgeReader 
		extends AccumuloProtoReader<Concrete.Edge, Concrete.Edge.Builder, Concrete.EdgeId, IndexedEdge> 
	{
		private final Graph.Reader graphReader;
		EdgeReader(Collection<Stage> stages, Graph.Reader graphReader) throws RebarException {
			super(accumuloConnector, edgesTableName, stages); 
			this.graphReader = graphReader;
		}
		protected Concrete.Edge.Builder getRootBuilder(Concrete.EdgeId edgeId) {
			return getCoreEdgeBuilder(edgeId);
		}
		protected Text toRowId(Concrete.EdgeId edgeId) { 
			return edgeIdToRowId(edgeId); }
		protected Concrete.EdgeId toIdentifier(Text rowId) { 
			return rowIdToEdgeId(rowId); }
		protected IndexedEdge wrap(Concrete.EdgeId edgeId, Concrete.Edge edge, Map<Key,Value> row, StageOwnership stageOwnership) 
			throws RebarException
		{
			return new IndexedEdge(edge, new ProtoIndex(edge), 
								   stageOwnership, graphReader); 
		}
	}

	/** Reader implementation */
	private class Reader implements Graph.Reader {
		private VertexReader vertexReader;
		private EdgeReader edgeReader;
		private final boolean loadStageOwnership;
		Reader(Collection<Stage> stages, boolean loadStageOwnership) throws RebarException {
			vertexReader = new VertexReader(stages, this); 
			edgeReader = new EdgeReader(stages, this);
			this.loadStageOwnership = loadStageOwnership;
		}
		// Vertex reading methods.
		@Override
		public IndexedVertex loadVertex(Concrete.UUID vertexId) throws RebarException {
			return vertexReader.read(vertexId, loadStageOwnership); }
		@Override
		public Iterator<IndexedVertex> loadVertices(Collection<Concrete.UUID> vertexIds) throws RebarException {
			return vertexReader.read(vertexIds, loadStageOwnership); }
		@Override
		public Iterator<IndexedVertex> loadVertices() throws RebarException {
			return vertexReader.read(lookupVertexIdSet("all"), loadStageOwnership); }

		// Edge reading methods.
		@Override
		public IndexedEdge loadEdge(Concrete.EdgeId edgeId) throws RebarException {
			IndexedEdge result = edgeReader.readIfExists(edgeId, loadStageOwnership); 
			if (result == null) {
				Concrete.Edge edge = getCoreEdgeBuilder(edgeId).build();
				StageOwnership stageOwnership = loadStageOwnership ? new StageOwnership() : null;
				result = new IndexedEdge(edge, new ProtoIndex(edge), 
										 stageOwnership, this);
			}
			return result;
		}
		@Override
		public IndexedEdge loadEdge(Concrete.DirectedEdgeId directedEdgeId) throws RebarException {
			Concrete.EdgeId edgeId = RebarIdUtil.buildEdgeId(directedEdgeId);
			IndexedEdge edge = loadEdge(edgeId);
			if (directedEdgeId.getDst().equals(edgeId.getV1()))
				edge = edge.reversed();
			return edge;
		}
		@Override
		public IndexedEdge loadEdge(Concrete.UUID src, Concrete.UUID dst) throws RebarException {
			return loadEdge(RebarIdUtil.buildDirectedEdgeId(src, dst)); }
		@Override
		public IndexedEdge loadEdge(IndexedVertex src, IndexedVertex dst) throws RebarException {
			return loadEdge(src.getUuid(), dst.getUuid()); }
		@Override
		public Iterator<IndexedEdge> loadEdges(Collection<Concrete.EdgeId> edgeIds) throws RebarException {
			return edgeReader.read(edgeIds, loadStageOwnership); }
		@Override
		public Iterator<IndexedEdge> loadEdges() throws RebarException {
			return edgeReader.read(lookupEdgeIdSet("all"), loadStageOwnership); }

		@Override
		public void close() throws RebarException {
			vertexReader.close(); 
			edgeReader.close();
		}
		@Override
		public Collection<Stage> getInputStages() throws RebarException {
			return vertexReader.getInputStages(); }
		@Override
		public Graph getGraph() { return AccumuloBackedGraph.this; }
	}

	// ======================================================================
	// Initializer
	// ======================================================================

	private class Initializer 
		extends AccumuloWriter 
		implements Graph.Initializer 
	{
		Initializer() throws RebarException { 
			super(accumuloConnector, verticesTableName); 
		}

		public IndexedVertex addVertex(Concrete.Vertex vertex) throws RebarException {
			if (vertex.getNeighborCount() > 0)
				throw new RebarException("Graph.Initailizer may not be used to add a vertex with"+
										 " neighbors; Edges must be added with a Graph.Writer.");
			Text vertexId = rowIdFor(vertex);
			if (DEBUG_LEVEL>0 && (rowExists(vertexId)))
				throw new RebarException("Vertex "+vertexId+" has already been added!");
			write(vertexId,
				  AccumuloBackedGraph.ROOT_CF,
				  AccumuloBackedGraph.EMPTY_CQ,
				  new Value(vertex.toByteArray()));
			summaryTable.incrementCount(getName(), "vertex");
			return new IndexedVertex(vertex, new ProtoIndex(vertex), null, null);
		}
	}

	// ======================================================================
	// Writer
	// ======================================================================
	
	private class Writer implements Graph.Writer 
	{ 
		private final AccumuloWriter vertexWriter;
		private final AccumuloWriter edgeWriter;
		private final Stage stage;

		public Writer(Stage stage) throws RebarException { 
			this.vertexWriter = new AccumuloWriter(accumuloConnector, verticesTableName);
			this.edgeWriter = new AccumuloWriter(accumuloConnector, edgesTableName);
			this.stage = stage;
		}

		public void close() throws RebarException {
			vertexWriter.close();
			edgeWriter.close();
		}

		public void flush() throws RebarException {
			vertexWriter.flush();
			edgeWriter.flush();
		}

		public Stage getOutputStage() { return stage; }

		public void saveVertex(IndexedVertex vertex) throws RebarException
		{
			Text rowId = rowIdFor(vertex.getProto());
			if (DEBUG_LEVEL>0 && (!vertexWriter.rowExists(rowId)))
				throw new RebarException("Vertex "+rowId+" not found in graph");
			vertexWriter.writeProtoModifications(stage, rowId, vertex.getIndex());
		}

		public void saveEdge(IndexedEdge edge) throws RebarException
		{
			Text rowId = rowIdFor(edge.getProto());
			edgeWriter.writeProtoModifications(stage, rowId, edge.getIndex());
			// Record the neighborship relationships implied by this edge.
			Text v1 = new Text(ByteUtil.fromUUID(edge.getEdgeId().getV1()));
			Text v2 = new Text(ByteUtil.fromUUID(edge.getEdgeId().getV2()));
			Text cf = new Text(ByteUtil.fromIntWithPrefix(NEIGHBOR_CF_PREFIX, stage.getStageId()));
			vertexWriter.write(v1, cf, v2, EMPTY_VALUE);
			vertexWriter.write(v2, cf, v1, EMPTY_VALUE);
		}
	}

	// ======================================================================
	// DiffWriter
	// ======================================================================
	
	private class DiffWriter implements Graph.DiffWriter 
	{ 
		private final AccumuloWriter vertexWriter;
		private final AccumuloWriter edgeWriter;
		private final Stage stage;

		public DiffWriter(Stage stage) throws RebarException { 
			this.vertexWriter = new AccumuloWriter(accumuloConnector, verticesTableName);
			this.edgeWriter = new AccumuloWriter(accumuloConnector, edgesTableName);
			this.stage = stage;
		}

		public void close() throws RebarException {
			vertexWriter.close();
			edgeWriter.close();
		}

		public void flush() throws RebarException {
			vertexWriter.flush();
			edgeWriter.flush();
		}

		public void saveVertexDiff(Concrete.UUID vertexId, Map<ProtoIndex.ModificationTarget, byte[]> changes) throws RebarException {
			Text rowId = vertexIdToRowId(vertexId);
			if (DEBUG_LEVEL>0 && (!vertexWriter.rowExists(rowId)))
				throw new RebarException("Vertex "+vertexId+" not found in graph");
			vertexWriter.writeProtoModifications(stage, rowId, changes);
		}

		public void saveEdgeDiff(Concrete.EdgeId edgeId, Map<ProtoIndex.ModificationTarget, byte[]> changes) throws RebarException {
			Text rowId = edgeIdToRowId(edgeId);
			edgeWriter.writeProtoModifications(stage, rowId, changes);
			// Record the neighborship relationships implied by this edge.
			Text v1 = new Text(ByteUtil.fromUUID(edgeId.getV1()));
			Text v2 = new Text(ByteUtil.fromUUID(edgeId.getV2()));
			Text cf = new Text(ByteUtil.fromIntWithPrefix(NEIGHBOR_CF_PREFIX, stage.getStageId()));
			vertexWriter.write(v1, cf, v2, EMPTY_VALUE);
			vertexWriter.write(v2, cf, v1, EMPTY_VALUE);
		}
	}

	// ======================================================================
	// Vertex Identifier Sets
	// ======================================================================

	private class VertexIdSetTable extends AccumuloIdSetTable<Concrete.UUID> {
		VertexIdSetTable(AccumuloConnector accumuloConnector, String tableName) {
			super(accumuloConnector, tableName); }
		protected Text idToRowId(Concrete.UUID id) { return vertexIdToRowId(id); }
		protected Concrete.UUID rowIdToId(Text id) { return rowIdToVertexId(id); }
	}

	private static class AllVertexRowIds extends AccumuloAllRowIdsCollection<Concrete.UUID> {
		AllVertexRowIds(AccumuloConnector accumuloConnector, String tableName) {
			super(accumuloConnector, tableName); }
		protected Concrete.UUID rowIdToId(Text rowid) { return rowIdToVertexId(rowid); }
	}

	@Override
	public Collection<Concrete.UUID> readVertexIdSet(File filename) throws RebarException {
		throw new RebarException("Not implemented yet");
	}

	@Override
	public void registerVertexIdSet(String name, Collection<Concrete.UUID> idSet) throws RebarException {
		if (name.equalsIgnoreCase("all"))
			throw new RebarException("The name 'all' is reserved and can not be redefined.");
		vertexIdSetTable.registerIdSet(name, idSet);
	}

	@Override
	public Collection<Concrete.UUID> lookupVertexIdSet(String name) throws RebarException {
		if (name.equalsIgnoreCase("all")) {
			return new AllVertexRowIds(accumuloConnector, verticesTableName);
		} else {
			return vertexIdSetTable.lookupIdSet(name);
		}
	}

	@Override
	public Collection<String> getVertexIdSetNames() throws RebarException {
		return vertexIdSetTable.getSubsetNames();
	}

	// ======================================================================
	// Edge Identifier Sets
	// ======================================================================

	private class EdgeIdSetTable extends AccumuloIdSetTable<Concrete.EdgeId> {
		EdgeIdSetTable(AccumuloConnector accumuloConnector, String tableName) {
			super(accumuloConnector, tableName); }
		protected Text idToRowId(Concrete.EdgeId id) { return edgeIdToRowId(id); }
		protected Concrete.EdgeId rowIdToId(Text id) { return rowIdToEdgeId(id); }
	}

	private static class AllEdgeRowIds extends AccumuloAllRowIdsCollection<Concrete.EdgeId> {
		AllEdgeRowIds(AccumuloConnector accumuloConnector, String tableName) {
			super(accumuloConnector, tableName); }
		protected Concrete.EdgeId rowIdToId(Text rowid) { return rowIdToEdgeId(rowid); }
	}

	@Override
	public Collection<Concrete.EdgeId> readEdgeIdSet(File filename) throws RebarException {
		throw new RebarException("Not implemented yet");
	}

	@Override
	public void registerEdgeIdSet(String name, Collection<Concrete.EdgeId> idSet) throws RebarException {
		if (name.equalsIgnoreCase("all"))
			throw new RebarException("The name 'all' is reserved and can not be redefined.");
		edgeIdSetTable.registerIdSet(name, idSet);
	}

	@Override
	public Collection<Concrete.EdgeId> lookupEdgeIdSet(String name) throws RebarException {
		if (name.equalsIgnoreCase("all")) {
			return new AllEdgeRowIds(accumuloConnector, edgesTableName);
		} else {
			return edgeIdSetTable.lookupIdSet(name);
		}
	}

	@Override
	public Collection<String> getEdgeIdSetNames() throws RebarException {
		return edgeIdSetTable.getSubsetNames();
	}

	@Override
	public Collection<Concrete.EdgeId> getEdgesWithOneVertexIn(Collection<Concrete.UUID> vertexIds) throws RebarException {
		throw new RebarException("NO IMPLEMENTED YET");
	}

	@Override
	public Collection<Concrete.EdgeId> getEdgesWithBothVerticesIn(Collection<Concrete.UUID> vertexIds) throws RebarException {
		throw new RebarException("NO IMPLEMENTED YET");
	}

	// ======================================================================
	// Helper Methods
	// ======================================================================
	
	private static Text vertexIdToRowId(Concrete.UUID id) {
		return new Text(ByteUtil.fromUUID(id));
	}

	private static Concrete.UUID rowIdToVertexId(Text rowId) {
		return ByteUtil.toUUID(rowId.getBytes());
	}

	private static Text edgeIdToRowId(Concrete.EdgeId id) {
		if ((id.getV1() == ZERO_UUID) || (id.getV2() == ZERO_UUID))
			throw new IllegalArgumentException("Attempt to write a edge with an empty uuid");
		if (!RebarIdUtil.edgeIdIsValid(id))
			throw new IllegalArgumentException("Attempt to write edge where v2<v1: "+id);
		byte[] rowid = new byte[32];
		ByteUtil.fromLong(id.getV1().getHigh(),   0, rowid);
		ByteUtil.fromLong(id.getV1().getLow(),    8, rowid);
		ByteUtil.fromLong(id.getV2().getHigh(),  16, rowid);
		ByteUtil.fromLong(id.getV2().getLow(),   24, rowid);
		return new Text(rowid);
	}

	private static Concrete.EdgeId rowIdToEdgeId(Text rowId) {
		// Will always return a normalized EdgeId.
		byte[] bytes = rowId.getBytes();
		Concrete.UUID v1 = Concrete.UUID.newBuilder()
			.setHigh(ByteUtil.toLong(bytes, 0))
			.setLow(ByteUtil.toLong(bytes, 8))
			.build();
		Concrete.UUID v2 = Concrete.UUID.newBuilder()
			.setHigh(ByteUtil.toLong(bytes, 16))
			.setLow(ByteUtil.toLong(bytes, 24))
			.build();
		return Concrete.EdgeId.newBuilder()
			.setV1(v1).setV2(v2).build();
	}

	private static Text rowIdFor(Concrete.Vertex vertex) throws RebarException {
		Concrete.UUID uuid = RebarIdUtil.getUUID(vertex);
		if (uuid == ZERO_UUID)
			throw new RebarException("Attempt to write a vertex with an empty uuid");
		return vertexIdToRowId(uuid);
	}

	private static Text rowIdFor(Concrete.Edge edge) throws RebarException {
		// Note: we do a sanity check that v1<v2 in edgeIdToRowId.
		return edgeIdToRowId(edge.getEdgeId());
	}

	// ======================================================================
	// Static Methods
	// ======================================================================

	public static boolean graphExists(String graphName) throws RebarException {
		AccumuloConnector accumuloConnector = new AccumuloConnector();
		boolean result = true;
		for (String suffix: TABLE_SUFFIXES) {
			if (!accumuloConnector.tableExists(TABLE_PREFIX+graphName+suffix))
				result = false;
		}
		accumuloConnector.release();
		return result;
	}

	public static Collection<String> listGraphs() throws RebarException {
		AccumuloConnector accumuloConnector = new AccumuloConnector();
		Map<String, Integer> graphTableCounts = new HashMap<String, Integer>();
		for (String tableName: accumuloConnector.listTables()) {
			if (tableName.startsWith(TABLE_PREFIX)) {
				for (String suffix: TABLE_SUFFIXES) {
					if (tableName.endsWith(suffix)) {
						int s = TABLE_PREFIX.length();
						int e = tableName.length()-suffix.length();
						String graphName = tableName.substring(s, e);
						Integer oldVal = graphTableCounts.get(graphName);
						if (oldVal==null) oldVal = 0;
						graphTableCounts.put(graphName, oldVal+1);
					}
				}
			}
		}
		Collection<String> result = new TreeSet<String>();
		for (Map.Entry<String,Integer> entry: graphTableCounts.entrySet()) {
			if (entry.getValue() == TABLE_SUFFIXES.length) {
				result.add(entry.getKey());
			} else {
				System.err.println("Warning: one or more tables appears to be "+
								   "missing for graph "+entry.getKey());
			}
		}
		accumuloConnector.release();
		return result;
	}

	public static void deleteGraph(String graphName) throws RebarException {
		AccumuloConnector accumuloConnector = new AccumuloConnector();
		for (String suffix: TABLE_SUFFIXES)
			accumuloConnector.deleteTable(TABLE_PREFIX+graphName+suffix);
		AccumuloSummaryTable summaryTable = new AccumuloSummaryTable(accumuloConnector, TABLE_PREFIX);
		summaryTable.deleteEntry(graphName);
		accumuloConnector.release();
	}

	protected Concrete.Edge.Builder getCoreEdgeBuilder(Concrete.EdgeId edgeId) {
		return Concrete.Edge.newBuilder()
			.setEdgeId(edgeId)
			.setUndirected(Concrete.UndirectedAttributes.newBuilder())
			.setV1ToV2(Concrete.DirectedAttributes.newBuilder())
			.setV2ToV1(Concrete.DirectedAttributes.newBuilder());
	}
}

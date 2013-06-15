/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.hlt.rebar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.protobuf.Descriptors.FieldDescriptor;

import edu.jhu.hlt.concrete.Concrete;
import edu.jhu.hlt.concrete.Concrete.CommunicationGUID;
import edu.jhu.hlt.concrete.Concrete.CommunicationGUIDAttribute;
import edu.jhu.hlt.concrete.Concrete.UUID;
import edu.jhu.hlt.concrete.Concrete.Vertex;
import edu.jhu.hlt.concrete.util.IdUtil;

public class IndexedKnowledgeGraph extends IndexedProto<Concrete.KnowledgeGraph> {
	private final FieldDescriptor VERTEX_FIELD = 
		Concrete.KnowledgeGraph.getDescriptor().findFieldByName("vertex");
	private final FieldDescriptor EDGE_FIELD = 
		Concrete.KnowledgeGraph.getDescriptor().findFieldByName("edge");
	private final FieldDescriptor NEIGHBOR_FIELD = 
		Concrete.Vertex.getDescriptor().findFieldByName("neighbor");

	//======================================================================
	// Private variables
	//======================================================================

	/** Maps EdgeId to Concrete.Edge object */
	private Map<Concrete.EdgeId, Concrete.Edge> edgeById = null;


	//======================================================================
	// Constructor
	//======================================================================
	
	public static IndexedKnowledgeGraph build(Concrete.UUID uuid, ProtoIndex index) throws RebarException
	{
		IndexedKnowledgeGraph cached = index.getIndexedProto(uuid);
		if (cached != null) return cached;
		else return new IndexedKnowledgeGraph((Concrete.KnowledgeGraph)index.lookup(uuid), index);
	}

	public static IndexedKnowledgeGraph build(Concrete.KnowledgeGraph graph, ProtoIndex index) throws RebarException {
		IndexedKnowledgeGraph cached = index.getIndexedProto(graph.getUuid());
		if (cached != null) return cached;
		else return new IndexedKnowledgeGraph(graph, index);

	}

	private IndexedKnowledgeGraph(Concrete.KnowledgeGraph graph, ProtoIndex index) 
		throws RebarException
	{
		super(graph, index);
	}

	//======================================================================
	// Sanity Checks
	//======================================================================

	/** For each edge in this graph, check that v1 is less than or
	 * equal to v2.  If not, then raise a RebarExceeption. */
	public void checkEdgeIds() throws RebarException {
		for (Concrete.Edge edge: getProto().getEdgeList()) {
			if (!IdUtil.edgeIdIsValid(edge.getEdgeId()))
				throw new RebarException("Attempt to save an edge with v2<v1");
		}
	}

	/** Check that the neighbor list for each vertex corresponds with
	 * the set of edges in the graph. */
	public void checkVertexNeighbors() throws RebarException {
		// Get the lists of neighbors from the edges.
		Map<Concrete.UUID, Set<Concrete.UUID>> neighbors = new HashMap<Concrete.UUID, Set<Concrete.UUID>>();
		for (Concrete.Edge edge: getProto().getEdgeList()) {
			Concrete.UUID v1 = edge.getEdgeId().getV1();
			Concrete.UUID v2 = edge.getEdgeId().getV2();
			if (!neighbors.containsKey(v1))
				neighbors.put(v1, new HashSet<Concrete.UUID>());
			if (!neighbors.containsKey(v2))
				neighbors.put(v2, new HashSet<Concrete.UUID>());
			neighbors.get(v1).add(v2);
			neighbors.get(v2).add(v1);
		}
		// Compare them to the lists in each vertex
		for (Concrete.Vertex vertex: getProto().getVertexList()) {
			Set<Concrete.UUID> vertexNeighbors = new HashSet<Concrete.UUID>(vertex.getNeighborList());
			if (!vertexNeighbors.equals(neighbors.get(vertex.getUuid())))
				throw new RebarException("The neighbor list for a vertex does not match " +
										 "the list of edges that connect to that vertex: " +
										 vertex.getUuid());
		}
	}

	//======================================================================
	// Indexing
	//======================================================================

	@Override
	protected void updateIndices() throws RebarException {
		edgeById = null;
	}



	//======================================================================
	// Modification Convenience Methods
	//======================================================================

	public IndexedVertex addVertex() throws RebarException {
		Concrete.Vertex vertex = Concrete.Vertex.newBuilder()
			.setUuid(IdUtil.generateUUID())
			.build();
		addField(protoObj, VERTEX_FIELD, vertex);
		return getVertex(IdUtil.getUUID(vertex));
	}

	//======================================================================
	// Indexed Child Objects
	//======================================================================

	/**
	 * Return a list of {@link IndexedVertex} objects
	 * that are contained in this IndexedKnowledgeGraph.
	 * 
	 * @return a {@link List} of {@link IndexedVertex} objects in this graph
	 * @throws RebarException
	 */
	public List<IndexedVertex> getVertices() throws RebarException {
	  List<IndexedVertex> vertices = new ArrayList<IndexedVertex>();
	  List<Vertex> vList = this.getProto().getVertexList();
	  List<UUID> uuidList = new ArrayList<UUID>(vList.size());
	  for (Vertex v : vList)
	    uuidList.add(v.getUuid());
	  for (UUID uuid : uuidList)
	    vertices.add(this.getVertex(uuid));
	  return vertices;
	}
	
	/** Return the specified vertex from this communication's
	 * knowledge graph. */
	public IndexedVertex getVertex(Concrete.UUID uuid) throws RebarException {
		return IndexedVertex.build(uuid, getIndex());
	}

	/** Return the specified edge from this communication's knowledge
	 * graph.  If it does not exist yet, then it is created and added
	 * to the knowledge graph. */
	public IndexedEdge getEdge(Concrete.DirectedEdgeId directedEdgeId) throws RebarException {
		// If we've already built an IndexedEdge for this edge, then return it.
		IndexedEdge cached = getIndex().getIndexedProto(directedEdgeId);
		if (cached != null) return cached;
		// Otherwise, check our index of Concrete.Edges by EdgeId.
		Concrete.EdgeId edgeId = IdUtil.buildEdgeId(directedEdgeId);
		updateEdgeByIdMap();
		final Concrete.Edge edge;
		if (edgeById.containsKey(edgeId)) {
			edge = edgeById.get(edgeId); // existing edge
		} else {
			edge = addEdge(edgeId); // new edge
		}
		// Wrap the rebar Edge in an IndexedEdge.
		Concrete.DirectedEdgeId.Direction direction = 
			IdUtil.getEdgeDirection(directedEdgeId);
		return IndexedEdge.build(edge, getIndex(), direction);
	}

	/** Return the specified edge from this communication's knowledge
	 * graph (using the V1_TO_V2 direction).  If it does not exist
	 * yet, then it is created and added to the knowledge graph. */
	public IndexedEdge getEdge(Concrete.EdgeId edgeId) throws RebarException {
		return getEdge(edgeId.getV1(), edgeId.getV2());
	}

	/** @see IndexedKnowledgeGraph#getEdge(Concrete.DirectedEdgeId) */
	public IndexedEdge getEdge(Concrete.UUID src, Concrete.UUID dst) throws RebarException {
		return getEdge(IdUtil.buildDirectedEdgeId(src, dst));
	}

	/** @see IndexedKnowledgeGraph#getEdge(Concrete.DirectedEdgeId) */
	public IndexedEdge getEdge(IndexedVertex src, IndexedVertex dst) throws RebarException {
		return getEdge(IdUtil.buildDirectedEdgeId(src.getUuid(), dst.getUuid()));
	}

	//======================================================================
	// Accessors
	//======================================================================

	/** Return true if an edge between the specified pair of vertices
	 * exists. */
	public boolean hasEdge(Concrete.EdgeId edgeId) throws RebarException {
		updateEdgeByIdMap();
		return (edgeById.containsKey(edgeId));
	}

	/** @see IndexedKnowledgeGraph#hasEdge(Concrete.EdgeId) */
	public boolean hasEdge(Concrete.DirectedEdgeId edgeId) throws RebarException {
		updateEdgeByIdMap();
		return hasEdge(IdUtil.buildEdgeId(edgeId));
	}

	/** @see IndexedKnowledgeGraph#hasEdge(Concrete.EdgeId) */
	public boolean hasEdge(Concrete.UUID v1, Concrete.UUID v2) throws RebarException {
		updateEdgeByIdMap();
		return hasEdge(IdUtil.buildEdgeId(v1, v2));
	}

	/** @see IndexedKnowledgeGraph#hasEdge(Concrete.EdgeId) */
//	public boolean hasEdge(IndexedVertex v1, IndexedVertex v2) throws RebarException {
//		updateEdgeByIdMap();
//		return hasEdge(IdUtil.buildEdgeId(v1, v2));
//	}

	/** Return the unique vertex in this knowledge graph that has the
	 * given communication id.  If there is no such vertex, or if
	 * there are multiple such vertices, then raise an exception. */
	public IndexedVertex getCommunicationVertex(String comId) throws RebarException {
	  IndexedVertex ivHolder = null;
	  for (IndexedVertex iv : this.getVertices()) {
	    if (!iv.getProto().getCommunicationGuidList().isEmpty()) {
	      CommunicationGUIDAttribute cga = iv.getProto().getCommunicationGuidList().get(0);
	      CommunicationGUID cg = cga.getValue();
	      if (cg.getCommunicationId().equals(comId))
	        if (ivHolder == null)
	          ivHolder = iv;
	        else
	          throw new RebarException("There was more than one vertex that had a matching GUID. Offenders: " + 
	              ivHolder.getProto().getCommunicationGuidList().get(0).getValue().getCommunicationId() + " and: " +
	              cg.getCommunicationId());
	    }
	  }
	  
	  if (ivHolder != null)
	    return ivHolder;
	  else
	    throw new RebarException("Did not find a vertex with communicationGUID: " + comId);
	}

	//======================================================================
	// Concrete.EdgeId->IndexedEdge map
	//======================================================================

	private void updateEdgeByIdMap() throws RebarException {
		if (edgeById == null) 
			edgeById = new HashMap<Concrete.EdgeId, Concrete.Edge>();
		List<Concrete.Edge> edgeList = getProto().getEdgeList();
		if (edgeById.size() == edgeList.size())
			return; // All edges are already indexed.
		for (Concrete.Edge edge: edgeList) {
			Concrete.EdgeId edgeId = edge.getEdgeId();
			assert(IdUtil.edgeIdIsValid(edgeId));
			edgeById.put(edgeId, edge);
		}
	}

	//======================================================================
	// Private Helper Methods
	//======================================================================

	/** Helper method for getEdge(), which creates edges on demand
	 * when they don't exist.  Note that it's ok if multiple stages
	 * independently create "the same" edge, since we have code in the
	 * Corpus.Reader that manually merges any duplicate edges. */
	private Concrete.Edge addEdge(Concrete.EdgeId edgeId) throws RebarException {
		// Edge does not exist yet -- create it.
		Concrete.Vertex v1 = (Concrete.Vertex)lookup(edgeId.getV1());
		Concrete.Vertex v2 = (Concrete.Vertex)lookup(edgeId.getV2());
		if ((v1==null) || (v2==null))
			throw new RebarException("One or both of the vertices connected by this "+
									 "edge are not in this IndexedCommunication's "+
									 "knowledge graph.");
		Concrete.Edge edge = Concrete.Edge.newBuilder()
			.setEdgeId(edgeId)
			.setUndirected(Concrete.UndirectedAttributes.newBuilder())
			.setV1ToV2(Concrete.DirectedAttributes.newBuilder())
			.setV2ToV1(Concrete.DirectedAttributes.newBuilder())
			.build();
		// Add it to the knowledge graph.
		addField(protoObj, EDGE_FIELD, edge);
		// Update the neighbor lists of the two vertices we just connected.
		addField(v1, NEIGHBOR_FIELD, v2.getUuid());
		addField(v2, NEIGHBOR_FIELD, v1.getUuid());
		// Return the new Edge object.
		return edge;
	}

}

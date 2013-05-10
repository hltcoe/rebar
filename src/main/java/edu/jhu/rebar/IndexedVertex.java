/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.rebar;

import java.util.List;

import com.google.protobuf.Descriptors.FieldDescriptor;

import edu.jhu.concrete.Concrete;

/** Use different subclasses for different vertex types?? */
public class IndexedVertex extends IndexedProto<Concrete.Vertex> {
	//======================================================================
	// Private variables
	//======================================================================

	// These two variables are only used if the vertex was loaded from
	// a distributed graph (not a communication's knowledge graph)
	private final StageOwnership stageOwnership;
	private final Graph.Reader graphReader;

	//======================================================================
	// Constructor (for use in a Communication KnowledgeGraph)
	//======================================================================

	public static IndexedVertex build(Concrete.UUID uuid, ProtoIndex index) throws RebarException {
		IndexedVertex cached = index.getIndexedProto(uuid);
		if (cached != null) return cached;
		else return new IndexedVertex((Concrete.Vertex)index.lookup(uuid), index);
	}

	public static IndexedVertex build(Concrete.Vertex vertex, ProtoIndex index) throws RebarException {
		IndexedVertex cached = index.getIndexedProto(vertex.getUuid());
		if (cached != null) return cached;
		else return new IndexedVertex(vertex, index);
	}

	public IndexedVertex(Concrete.Vertex vertex, ProtoIndex index)
		throws RebarException
	{
		super(vertex, index);
		this.stageOwnership = null;
		this.graphReader = null;
	}


	//======================================================================
	// Constructor (for use in a Distributed Graph)
	//======================================================================

	public IndexedVertex(Concrete.Vertex vertex, ProtoIndex index, StageOwnership stageOwnership, Graph.Reader graphReader) 
		throws RebarException
	{
		super(vertex, index);
		this.stageOwnership = stageOwnership;
		this.graphReader = graphReader;
	}

	//======================================================================
	// Modification Methods
	//======================================================================

	public void addAttribute(FieldDescriptor field, Object value, Concrete.AttributeMetadata metadata) throws RebarException {
		addField(field, AttribUtil.buildAttribute(field, value, metadata));
	}

	public void addAttribute(FieldDescriptor field, Object value, String toolName) throws RebarException {
		addField(field, AttribUtil.buildAttribute(field, value, toolName));
	}

	public void addAttribute(FieldDescriptor field, Object value, String toolName, float confidence) throws RebarException {
		addField(field, AttribUtil.buildAttribute(field, value, toolName, confidence));
	}

	//======================================================================
	// Other Accessors
	//======================================================================

	public Concrete.UUID getUuid() {
		return getProto().getUuid();
	}

	//======================================================================
	// Neighbors
	//======================================================================

	public List<Concrete.UUID> getNeighborsIds() {
		return getProto().getNeighborList();
	}

	//======================================================================
	// Stage Ownership
	//======================================================================

	/** Return the stage ownership map for this indexed communication.
	 * This will be NULL unless you read the communication using
	 * Corpus.Reader.getCommunicationWithStageOwnership(). */
	public StageOwnership getStageOwnership() {
		return this.stageOwnership;
	}

	//======================================================================
	// Edges
	//======================================================================

	/*
	public List<IndexedEdge> getOutgoingEdges() {
		Concrete.UUID src = getUuid();
		for (Concrete.UUID dst: getProto().getNeighborList()) {
			boolean reversed = ();
		}
	}
	
	private IndexedEdge getEdge(Concrete.UUID src, Concrete.UUID dst) {
		Concrete.EdgeId edgeId = IdUtil.buildEdgeId(src, dst);
		if (graphReader != null) {
			return graphReader.readEdge(edgeId);
		} else {
			foo;
		}
	}
	*/
		

}

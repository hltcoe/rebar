/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.rebar;

import com.google.protobuf.Descriptors.FieldDescriptor;

import edu.jhu.hlt.concrete.Concrete;
import edu.jhu.hlt.concrete.ConcreteException;
import edu.jhu.hlt.concrete.util.AttribUtil;
import edu.jhu.hlt.concrete.util.IdUtil;

public class IndexedEdge extends IndexedProto<Concrete.Edge> {
    // ======================================================================
    // Private variables
    // ======================================================================

    /** V1_TO_V2 or V2_TO_V1 */
    private final Concrete.DirectedEdgeId.Direction direction;

    // These two variables are only used if the edge was loaded from
    // a distributed graph (not a communication's knowledge graph)
    private final StageOwnership stageOwnership;
    private final Graph.Reader graphReader;

    // ======================================================================
    // Constructor (for use in a Communication KnowledgeGraph)
    // ======================================================================

    public static IndexedEdge build(Concrete.DirectedEdgeId edgeId, ProtoIndex index) throws RebarException {
        IndexedEdge cached = index.getIndexedProto(edgeId);
        if (cached != null) {
            return cached;
        } else {
            IndexedKnowledgeGraph graph = ((IndexedCommunication) index.getRoot()).getKnowledgeGraph();
            return graph.getEdge(edgeId);
        }
    }

    public static IndexedEdge build(Concrete.Edge edge, ProtoIndex index, Concrete.DirectedEdgeId.Direction direction)
            throws RebarException {
        IndexedEdge cached = index.getIndexedProto(IdUtil.buildDirectedEdgeId(edge.getEdgeId(), direction));
        if (cached != null)
            return cached;
        else
            return new IndexedEdge(edge, index, direction);
    }

    public IndexedEdge(Concrete.Edge edge, ProtoIndex index, Concrete.DirectedEdgeId.Direction direction) throws RebarException {
        super(edge, index);
        this.direction = direction;
        this.stageOwnership = null;
        this.graphReader = null;
    }

    /**
     * Return a reversed view of this edge (where src and dst are swapped).
     * I.e., if this IndexedEdge's direction is V1_TO_V2, then return a new
     * IndexedEdge for the same edge but with direction V2_TO_V1; and vice
     * versa.
     */
    public IndexedEdge reversed() throws RebarException {
        Concrete.DirectedEdgeId.Direction revDir = (direction == Concrete.DirectedEdgeId.Direction.V1_TO_V2 ? Concrete.DirectedEdgeId.Direction.V2_TO_V1
                : Concrete.DirectedEdgeId.Direction.V1_TO_V2);
        return build(getProto(), getIndex(), revDir);
    }

    // ======================================================================
    // Constructor (for use in a Distributed Graph)
    // ======================================================================

    public IndexedEdge(Concrete.Edge edge, ProtoIndex index, StageOwnership stageOwnership, Graph.Reader graphReader) throws RebarException {
        super(edge, index);
        this.direction = Concrete.DirectedEdgeId.Direction.V1_TO_V2;
        this.stageOwnership = stageOwnership;
        this.graphReader = graphReader;
    }

    protected void registerIndexedProto() throws RebarException {
        getIndex().registerIndexedProto(this.getEdgeId(), this);
    }

    public Concrete.DirectedEdgeId getDirectedEdgeId() {
        return Concrete.DirectedEdgeId.newBuilder().setSrc(getSrcUuid()).setDst(getDstUuid()).build();
    }

    public Concrete.EdgeId getEdgeId() {
        return Concrete.EdgeId.newBuilder().setV1(getV1()).setV2(getV2()).build();
    }

    public Concrete.DirectedEdgeId.Direction getDirection() {
        return direction;
    }

    // ======================================================================
    // Modification Methods
    // ======================================================================

    // ////////////////////////
    // Undirected Attributes
    // ////////////////////////
    public void addUndirectedAttribute(FieldDescriptor field, Object value, Concrete.AttributeMetadata metadata) throws RebarException {
        try {
            addField(getUndirectedAttributes(), field, AttribUtil.buildAttribute(field, value, metadata));
        } catch (ConcreteException e) {
            throw new RebarException(e);
        }
    }

    public void addUndirectedAttribute(FieldDescriptor field, Object value, String toolName) throws RebarException {
        try {
            addField(getUndirectedAttributes(), field, AttribUtil.buildAttribute(field, value, toolName));
        } catch (ConcreteException e) {
            throw new RebarException(e);
        }
    }

    public void addUndirectedAttribute(FieldDescriptor field, Object value, String toolName, float confidence) throws RebarException {
        try {
            addField(getUndirectedAttributes(), field, AttribUtil.buildAttribute(field, value, toolName, confidence));
        } catch (ConcreteException e) {
            throw new RebarException(e);
        }
    }

    // ////////////////////////
    // Forward Attributes
    // ////////////////////////

    public void addForwardAttribute(FieldDescriptor field, Object value, Concrete.AttributeMetadata metadata) throws RebarException {
        try {
            addField(getForwardAttributes(), field, AttribUtil.buildAttribute(field, value, metadata));
        } catch (ConcreteException e) {
            throw new RebarException(e);
        }
    }

    public void addForwardAttribute(FieldDescriptor field, Object value, String toolName) throws RebarException {
        try {
            addField(getForwardAttributes(), field, AttribUtil.buildAttribute(field, value, toolName));
        } catch (ConcreteException e) {
            throw new RebarException(e);
        }
    }

    public void addForwardAttribute(FieldDescriptor field, Object value, String toolName, float confidence) throws RebarException {
        try {
            addField(getForwardAttributes(), field, AttribUtil.buildAttribute(field, value, toolName, confidence));
        } catch (ConcreteException e) {
            throw new RebarException(e);
        }
    }

    // ////////////////////////
    // Backward Attributes
    // ////////////////////////

    public void addBackwardAttribute(FieldDescriptor field, Object value, Concrete.AttributeMetadata metadata) throws RebarException {
        try {
            addField(getBackwardAttributes(), field, AttribUtil.buildAttribute(field, value, metadata));
        } catch (ConcreteException e) {
            throw new RebarException(e);
        }
    }

    public void addBackwardAttribute(FieldDescriptor field, Object value, String toolName) throws RebarException {
        try {
            addField(getBackwardAttributes(), field, AttribUtil.buildAttribute(field, value, toolName));
        } catch (ConcreteException e) {
            throw new RebarException(e);
        }
    }

    public void addBackwardAttribute(FieldDescriptor field, Object value, String toolName, float confidence) throws RebarException {
        try {
            addField(getBackwardAttributes(), field, AttribUtil.buildAttribute(field, value, toolName, confidence));
        } catch (ConcreteException e) {
            throw new RebarException(e);
        }
    }

    // ////////////////////////
    // Directed Attributes
    // ////////////////////////

    public void addDirectedAttribute(Concrete.UUID src, FieldDescriptor field, Concrete.UUID dst, Object value,
            Concrete.AttributeMetadata metadata) throws RebarException {
        try {
            addField(getDirectedAttributes(src, dst), field, AttribUtil.buildAttribute(field, value, metadata));
        } catch (ConcreteException e) {
            throw new RebarException(e);
        }
    }

    public void addDirectedAttribute(Concrete.UUID src, FieldDescriptor field, Concrete.UUID dst, Object value, String toolName)
            throws RebarException {
        try {
            addField(getDirectedAttributes(src, dst), field, AttribUtil.buildAttribute(field, value, toolName));
        } catch (ConcreteException e) {
            throw new RebarException(e);
        }
    }

    public void addDirectedAttribute(Concrete.UUID src, FieldDescriptor field, Concrete.UUID dst, Object value, String toolName,
            float confidence) throws RebarException {
        try {
            addField(getDirectedAttributes(src, dst), field, AttribUtil.buildAttribute(field, value, toolName, confidence));
        } catch (ConcreteException e) {
            throw new RebarException(e);
        }
    }

    public void addDirectedAttribute(IndexedVertex src, FieldDescriptor field, IndexedVertex dst, Object value,
            Concrete.AttributeMetadata metadata) throws RebarException {
        try {
            addField(getDirectedAttributes(src.getUuid(), dst.getUuid()), field, AttribUtil.buildAttribute(field, value, metadata));
        } catch (ConcreteException e) {
            throw new RebarException(e);
        }
    }

    public void addDirectedAttribute(IndexedVertex src, FieldDescriptor field, IndexedVertex dst, Object value, String toolName)
            throws RebarException {
        try {
            addField(getDirectedAttributes(src.getUuid(), dst.getUuid()), field, AttribUtil.buildAttribute(field, value, toolName));
        } catch (ConcreteException e) {
            throw new RebarException(e);
        }
    }

    public void addDirectedAttribute(IndexedVertex src, FieldDescriptor field, IndexedVertex dst, Object value, String toolName,
            float confidence) throws RebarException {
        try {
            addField(getDirectedAttributes(src.getUuid(), dst.getUuid()), field,
                    AttribUtil.buildAttribute(field, value, toolName, confidence));
        } catch (ConcreteException e) {
            throw new RebarException(e);
        }
    }

    // ======================================================================
    // Source & Destination
    // ======================================================================

    /** Return the source vertex of this edge. */
    public IndexedVertex getSrc() throws RebarException {
        return getVertexByUuid(getSrcUuid());
    }

    /** Return the destination vertex of this edge. */
    public IndexedVertex getDst() throws RebarException {
        return getVertexByUuid(getDstUuid());
    }

    private IndexedVertex getVertexByUuid(Concrete.UUID uuid) throws RebarException {
        if (graphReader != null)
            return graphReader.loadVertex(uuid);
        else
            return IndexedVertex.build(uuid, getIndex());
    }

    /** Return the UUID of the source vertex of this edge. */
    public Concrete.UUID getSrcUuid() {
        return ((direction == Concrete.DirectedEdgeId.Direction.V1_TO_V2) ? getProto().getEdgeId().getV1() : getProto().getEdgeId().getV2());
    }

    /** Return the UUID of the destination vertex of this edge. */
    public Concrete.UUID getDstUuid() {
        return ((direction == Concrete.DirectedEdgeId.Direction.V1_TO_V2) ? getProto().getEdgeId().getV2() : getProto().getEdgeId().getV1());
    }

    // ======================================================================
    // Attributes
    // ======================================================================

    /** Return the undirected attributes of this edge. */
    public Concrete.UndirectedAttributes getUndirectedAttributes() {
        return getProto().getUndirected();
    }

    /**
     * Return the DirectedAttributes that go from this edge's source vertex to
     * its destination vertex.
     */
    public Concrete.DirectedAttributes getForwardAttributes() {
        return ((direction == Concrete.DirectedEdgeId.Direction.V1_TO_V2) ? getProto().getV1ToV2() : getProto().getV2ToV1());
    }

    /**
     * Return the DirectedAttributes that go from this edge's destination vertex
     * to its source vertex.
     */
    public Concrete.DirectedAttributes getBackwardAttributes() {
        return ((direction == Concrete.DirectedEdgeId.Direction.V1_TO_V2) ? getProto().getV2ToV1() : getProto().getV1ToV2());
    }

    /**
     * Return this edge's directed attributes going in the specified direction.
     * "src" and "dst" must be the vertices of this edge.
     */
    public Concrete.DirectedAttributes getDirectedAttributes(Concrete.UUID src, Concrete.UUID dst) throws RebarException {
        Concrete.UUID v1 = protoObj.getEdgeId().getV1();
        Concrete.UUID v2 = protoObj.getEdgeId().getV2();
        if ((src.equals(v1)) && (dst.equals(v2)))
            return protoObj.getV1ToV2();
        else if ((src.equals(v2)) && (dst.equals(v1)))
            return protoObj.getV2ToV1();
        else
            throw new RebarException("src and dst must be the two vertices of the edge");
    }

    /**
     * Return this edge's directed attributes going in the specified direction.
     * "src" and "dst" must be the vertices of this edge.
     */
    public Concrete.DirectedAttributes getDirectedAttributes(IndexedVertex src, IndexedVertex dst) throws RebarException {
        return getDirectedAttributes(src.getUuid(), dst.getUuid());
    }

    /**
     * Return this edge's directed attributes going in the specified direction.
     * "src" and "dst" must be the vertices of this edge.
     */
    public Concrete.DirectedAttributes getDirectedAttributes(Concrete.Vertex src, Concrete.Vertex dst) throws RebarException {
        return getDirectedAttributes(src.getUuid(), dst.getUuid());
    }

    // ======================================================================
    // V1 & V2
    // ======================================================================

    /**
     * Return the numerically lesser UUID of the two vertices connected by this
     * edge.
     */
    public Concrete.UUID getV1() {
        return getProto().getEdgeId().getV1();
    }

    /**
     * Return the numerically greater UUID of the two vertices connected by this
     * edge.
     */
    public Concrete.UUID getV2() {
        return getProto().getEdgeId().getV2();
    }

    // ======================================================================
    // Stage Ownership
    // ======================================================================

    /**
     * Return the stage ownership map for this indexed communication. This will
     * be NULL unless you read the communication using
     * Corpus.Reader.getCommunicationWithStageOwnership().
     */
    public StageOwnership getStageOwnership() {
        return this.stageOwnership;
    }

}

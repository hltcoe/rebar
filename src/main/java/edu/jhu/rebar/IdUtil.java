/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.rebar;

import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import edu.jhu.concrete.Concrete;

/**
 * Utility class for converting and generating protobuf UUIDs.
 */
public class IdUtil {

    /** Generate and return a random new protobuf UUID */
    public static Concrete.UUID generateUUID() {
        return fromJavaUUID(java.util.UUID.randomUUID());
    }
    
    /**
     * Given a {@link java.util.UUID} object, return a {@link Concrete.UUID} object.
     * 
     * @param uuid - a {@link java.util.UUID} object
     * @return a {@link Concrete.UUID} object
     */
    public static Concrete.UUID fromJavaUUID(java.util.UUID uuid) {
        return Concrete.UUID.newBuilder().setHigh(uuid.getMostSignificantBits())
                .setLow(uuid.getLeastSignificantBits())
                .build();
    }
    
    /**
     * Given a UUID String, return a {@link Concrete.UUID} object.
     * 
     * @param uuidString - valid UUID string
     * @return a {@link Concrete.UUID} object
     */
    public static Concrete.UUID fromUUIDString(String uuidString) {
        return fromJavaUUID(java.util.UUID.fromString(uuidString));
    }

    /** Return a standard string representation of the given UUID */
    public static String uuidToString(Concrete.UUID uuid) {
        java.util.UUID javaUuid = new java.util.UUID(uuid.getHigh(), uuid.getLow());
        return javaUuid.toString();
    }

    /**
     * Return a new EdgeId for the (undirected) edge connecting the given pair
     * of vertices. The two vertex UUIDs will be sorted before the EdgeId is
     * created, to ensure that v1 is the lesser UUID and v1 is the greater UUID
     * (as is required by the EdgeId type). If you wish to create an identifier
     * that identifies an edge and an associated direction, then use
     * buildDirectedEdgeId instead.
     */
    public static Concrete.EdgeId buildEdgeId(Concrete.UUID v1, Concrete.UUID v2) {
        if (uuidsAreOrdered(v1, v2)) {
            return Concrete.EdgeId.newBuilder().setV1(v1).setV2(v2).build();
        } else {
            return Concrete.EdgeId.newBuilder().setV1(v2).setV2(v1).build();
        }
    }

    /** @see IdUtil#buildEdgeId(Concrete.UUID, Concrete.UUID) */
    public static Concrete.EdgeId buildEdgeId(Concrete.Vertex v1, Concrete.Vertex v2) {
        return buildEdgeId(v1.getUuid(), v2.getUuid());
    }

    /** @see IdUtil#buildEdgeId(Concrete.UUID, Concrete.UUID) */
    public static Concrete.EdgeId buildEdgeId(IndexedVertex v1, IndexedVertex v2) {
        return buildEdgeId(v1.getUuid(), v2.getUuid());
    }

    /**
     * Return a new EdgeId for the undirected edge identified by the given
     * directed edge id. This essentially "strips off" the direction
     * information, by storing the two edge vertex UUIDs in a canonical order.
     */
    public static Concrete.EdgeId buildEdgeId(Concrete.DirectedEdgeId edgeId) {
        return buildEdgeId(edgeId.getSrc(), edgeId.getDst());
    }

    /**
     * Return true if the given edgeId is valid. An edgeId is invalid if v2 is
     * lesser than v1.
     */
    public static boolean edgeIdIsValid(Concrete.EdgeId edgeId) {
        return uuidsAreOrdered(edgeId.getV1(), edgeId.getV2());
    }

    /**
     * Return a new DirectedEdgeId for the given source and destination
     * vertices. Note: if you wish to create an identifier that identifies an
     * edge *without* an associated direction, then use buildEdgeId instead.
     */
    public static Concrete.DirectedEdgeId buildDirectedEdgeId(Concrete.UUID src, Concrete.UUID dst) {
        return Concrete.DirectedEdgeId.newBuilder().setSrc(src).setDst(dst).build();
    }

    /** @see IdUtil#buildDirectedEdgeId(Concrete.UUID, Concrete.UUID) */
    public static Concrete.DirectedEdgeId buildDirectedEdgeId(Concrete.Vertex src, Concrete.Vertex dst) {
        return buildDirectedEdgeId(src.getUuid(), dst.getUuid());
    }

    /** @see IdUtil#buildDirectedEdgeId(Concrete.UUID, Concrete.UUID) */
    public static Concrete.DirectedEdgeId buildDirectedEdgeId(IndexedVertex src, IndexedVertex dst) {
        return buildDirectedEdgeId(src.getUuid(), dst.getUuid());
    }

    /**
     * Return a new DirectedEdgeId for the given edge, with the specified
     * direction. If direction is V1_TO_V2, then edgeId.v1 will be used as the
     * source and edgeId.v2 will be used as the destination. If direction is
     * V2_TO_V1, then edgeId.v2 will be used as the source and edgeId.v1 will be
     * used as the destination.
     */
    public static Concrete.DirectedEdgeId buildDirectedEdgeId(Concrete.EdgeId edgeId,
            Concrete.DirectedEdgeId.Direction direction) {
        if (direction == Concrete.DirectedEdgeId.Direction.V1_TO_V2) {
            return Concrete.DirectedEdgeId.newBuilder().setSrc(edgeId.getV1()).setDst(edgeId.getV2()).build();
        } else {
            return Concrete.DirectedEdgeId.newBuilder().setSrc(edgeId.getV2()).setDst(edgeId.getV1()).build();
        }
    }

    public static Concrete.DirectedEdgeId.Direction getEdgeDirection(Concrete.DirectedEdgeId edgeId) {
        return ((uuidsAreOrdered(edgeId.getSrc(), edgeId.getDst())) ? Concrete.DirectedEdgeId.Direction.V1_TO_V2
                : Concrete.DirectedEdgeId.Direction.V2_TO_V1);
    }

    /**
     * Return true if the UUID v1 is lesser than or equal to the UUID v2.
     */
    public static boolean uuidsAreOrdered(Concrete.UUID v1, Concrete.UUID v2) {
        // Implementation note: Java has no unsigned data types. We
        // need to be careful here to make sure that we're doing the
        // equivalent of an unsigned comparison.
        return (MathUtil.unsignedLessThan(v1.getHigh(), v2.getHigh()) || ((v1.getHigh() == v2.getHigh()) && !MathUtil
                .unsignedLessThan(v2.getLow(), v1.getLow())));
    }

    /**
     * Return a java UUID extracted from the "uuid" field of the given protobuf
     * object. Throw RebarException if the given protobuf object has no "uuid"
     * field.
     */
    public static Concrete.UUID getUUID(com.google.protobuf.Message m) throws RebarException {
        Concrete.UUID result = getUUIDOrNull(m);
        if (result == null)
            throw new RebarException("Message has no 'uuid' field!");
        return result;
    }

    /**
     * Return a java UUID extracted from the "uuid" field of the given protobuf
     * object. Return null if the given protobuf object has no "uuid" field.
     */
    public static Concrete.UUID getUUIDOrNull(com.google.protobuf.Message m) {
        Descriptor desc = m.getDescriptorForType();
        FieldDescriptor uuidField = uuidFields.get(desc);
        // If uuidField is null, then either we haven't seen this
        // Descriptor before, or we've already seen it and decided
        // that it has no uuid field.
        if (uuidField == null) {
            if (!uuidFields.containsKey(desc)) {
                uuidField = desc.findFieldByName("uuid");
                uuidFields.put(desc, uuidField);
            }
        }
        // If m has a uuid field, then check it.
        if (uuidField != null) {
            Concrete.UUID uuid = (Concrete.UUID) m.getField(uuidField);
            if (uuid != null)
                return uuid;
        }
        // Message type has no uuid field, or uuid field is empty.
        return null;
    }

    private static Map<Descriptor, FieldDescriptor> uuidFields = new HashMap<Descriptor, FieldDescriptor>();

    /**
     * Return a java UUID extracted from the "uuid" field of the given
     * Communication.
     */
    public static Concrete.UUID getUUID(Concrete.Communication m) {
        return m.getUuid();
    }

}

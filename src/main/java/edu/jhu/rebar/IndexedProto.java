/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.rebar;

import java.util.IdentityHashMap;
import java.util.Map;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import edu.jhu.hlt.concrete.Concrete;
import edu.jhu.hlt.concrete.util.IdUtil;

public abstract class IndexedProto<ProtoObj extends Message> {
	//======================================================================
	// Private Variables
	//======================================================================
	/** The index that backs this IndexedProto. */
	private ProtoIndex index;

	/** The indexed protobuf object itself */
	protected ProtoObj protoObj;

	//======================================================================
	// Constructor
	//======================================================================

	public IndexedProto(ProtoObj protoObj, ProtoIndex index) throws RebarException {
		this.index = index;
		this.protoObj = protoObj;
		registerCallback();
		registerIndexedProto();
	}

	protected void registerIndexedProto() throws RebarException {
		index.registerIndexedProto(this.getUUID(), this);
	}

	protected void registerCallback() throws RebarException{
		index.registerCallback(protoObj, new ProtoIndex.ReplaceProtoCallback() {
			public void replace(Message oldMsg, Message newMsg) throws RebarException {
				assert(oldMsg==protoObj);
				@SuppressWarnings("unchecked")
				ProtoObj newProtoObj = (ProtoObj)newMsg;
				protoObj = newProtoObj;
				updateIndices();
			}
		});
	}

	protected void updateIndices() throws RebarException {}

	//======================================================================
	// Modification Methods
	//======================================================================

	/** Append a new value to a specified repeated field in given
	 * target message.  The target message must be contained in this
	 * indexed protobuf object, and must have a UUID. */
	public void addField(Message target, FieldDescriptor field, Message fieldValue) throws RebarException {
		index.addField(target, field, fieldValue);
	}

	/** Set the value of a specified optional field in given target
	 * message.  The field must not already have a value.  The target
	 * message must be contained in this indexed protobuf object, and
	 * must have a UUID. */
	public void setField(Message target, FieldDescriptor field, Message fieldValue) throws RebarException {
		index.setField(target, field, fieldValue);
	}

	/** Append a new value to a specified repeated field in this
	 * indexed protobuf object. */
	public void addField(FieldDescriptor field, Message fieldValue) throws RebarException {
		addField(protoObj, field, fieldValue); 
	}
	
	/** Set the value of a specified optional field in this
	 * indexed protobuf object. */
	public void setField(FieldDescriptor field, Message fieldValue) throws RebarException {
		setField(protoObj, field, fieldValue); 
	}
	
	//======================================================================
	// Accessors
	//======================================================================

	/** Return the protobuf object within this communication that has
	 * the specified UUID, or null if no such object is found. */
	public Message lookup(Concrete.UUID uuid) throws RebarException {
		return index.lookup(uuid);
	}

	/** Return the protobuf object wrapped by this indexed protobuf
	 * object. */
	public ProtoObj getProto() {
		return protoObj;
	}

	/** Return the index that backs this indexed protobuf object. */
	public ProtoIndex getIndex() {
		return index;
	}

	public Concrete.UUID getUUID() throws RebarException {
		return IdUtil.getUUID(protoObj);
	}

	public String toString() {
		return (protoObj.getDescriptorForType().getFullName()+"\n"+
				protoObj.toString());
	}

	//======================================================================
	// Private Variables
	//======================================================================

	/** A list of modifications that have been made but that have not
	 * yet been saved to the Corpus/Graph.  This variable maps from UUIDs of
	 * modified objects to serialized protobuf messages. */
	private Map<Concrete.UUID, byte[]> unsavedModifications = null;

	/** Index of all messages, by UUID */
	private Map<Concrete.UUID, Message> uuidIndex = null;

	/** Pointers from messages to their parents. */
	private IdentityHashMap<Message, Message> parentIndex = null;

}

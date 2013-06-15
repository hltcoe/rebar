/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.hlt.rebar;

/* Things I might want to add:

   - parent pointers -- e.g., given a tokenization, what sentence does
     it come from?  This may only be practical for objects that have
     uuids -- i.e., not for tokens or parse constituents??
   - complain vociferously about duplicate uuids
   - getTranslatedSentences()
   - parse tree navigation
   - get text for arbitrary(ish) element
     - from spans or from tokens

   - IndexedTokenization, IndexedParse, etc??

 */

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.protobuf.Descriptors.FieldDescriptor;

import edu.jhu.hlt.concrete.Concrete;

/** A wrapper for a Communication that tracks modifications and
 * provides automatically built indices.
 */
public class IndexedCommunication extends IndexedProto<Concrete.Communication> {
	private final FieldDescriptor SECTION_SEGMENTATION_FIELD = 
		Concrete.Communication.getDescriptor().findFieldByName("section_segmentation");
	private final FieldDescriptor LANGUAGE_ID_FIELD = 
		Concrete.Communication.getDescriptor().findFieldByName("language_id");
	private final FieldDescriptor ENTITY_MENTION_SET_FIELD = 
		Concrete.Communication.getDescriptor().findFieldByName("entity_mention_set");

	//======================================================================
	// Private variables
	//======================================================================

	private final StageOwnership stageOwnership;

	//======================================================================
	// Constructor
	//======================================================================
	
	public IndexedCommunication(Concrete.Communication comm, ProtoIndex index, StageOwnership stageOwnership) 
		throws RebarException
	{
		super(comm, index);
		if (comm != index.getRoot()) {
			//System.err.println("Comm="+comm);
			//System.err.println("root="+index.getRoot());
			throw new RebarException("Expected index root to be comm");
		}
		this.stageOwnership = stageOwnership;
	}

	//======================================================================
	// Modification Convenience Methods
	//======================================================================

	/** Add a new LangaugeIdentification to this communication. */
	public void addLangaugeId(Concrete.LanguageIdentification lid) throws RebarException {
		addField(LANGUAGE_ID_FIELD, lid);
	}

	public void addEntityMentionSet(Concrete.EntityMentionSet emset) throws RebarException {
		addField(ENTITY_MENTION_SET_FIELD, emset);
	}

	//======================================================================
	// Section Segmentations
	//======================================================================

	/** Add a new SectionSegmentation to this communication. */
	public void addSectionSegmentation(Concrete.SectionSegmentation segmentation) throws RebarException {
		addField(SECTION_SEGMENTATION_FIELD, segmentation);
	}

    int getSectionSegmentationCount() {
		return getProto().getSectionSegmentationCount();
	}

	/** Return an IndexedSectionSegmentation for the section
	 * segmentation with the given index. */
	public IndexedSectionSegmentation getSectionSegmentation(int index) throws RebarException {
		return IndexedSectionSegmentation.build(getProto().getSectionSegmentation(index), getIndex());
	}

	/** Return a list of IndexedSectionSegmentations for the section
	 * segmentations in this communication. */
	public List<IndexedSectionSegmentation> getSectionSegmentationList() throws RebarException {
		List<IndexedSectionSegmentation> result = new ArrayList<IndexedSectionSegmentation>();
		for (Concrete.SectionSegmentation seg: getProto().getSectionSegmentationList())
			result.add(IndexedSectionSegmentation.build(seg, getIndex()));
		return result;
	}

	/** Return an IndexedSectionSegmentation for the unique section
	 * segmentation for this communication.  If this communication has
	 * no section segmentation, or has multiple section segmentations,
	 * then throw an exception. */
	public IndexedSectionSegmentation getSectionSegmentation() throws RebarException {
		if (getProto().getSectionSegmentationCount() == 1)
			return getSectionSegmentation(0);
		else if (getProto().getSectionSegmentationCount() == 0)
			throw new RebarException("This communication has no section segmentation");
		else
			throw new RebarException("This communication has multiple section segmentations");
	}

	//======================================================================
	// Sentences
	//======================================================================

	/** Return a list of all IndexedSections for the sections in this
	 * communication, using the unique section segmentations.  If this
	 * communication does not have a unique section segmentation, then
	 * throw an exception.
	 */
	public List<IndexedSection> getSections() throws RebarException {
		return getSectionSegmentation().getSectionList();
	}

	/** Return a list of all IndexedSentences for the sentences in
	 * this communication, using the unique section segmentations and
	 * sentence segmentations.  If this communication does not have a
	 * unique section segmentation, or any section does not have a
	 * unique sentence segmentation, then throw an exception.
	 */
	public List<IndexedSentence> getSentences() throws RebarException {
		List<IndexedSentence> result = new ArrayList<IndexedSentence>();
		for (IndexedSection sec: getSections())
			result.addAll(sec.getSentences());
		return result;
	}

	//======================================================================
	// Knowledge Graph
	//======================================================================

	public IndexedKnowledgeGraph getKnowledgeGraph() throws RebarException {
		return IndexedKnowledgeGraph.build(getProto().getKnowledgeGraph(), getIndex());
	}

	//======================================================================
	// Other Accessors
	//======================================================================

	public String getText() {
		return protoObj.getText(); 
	}

	public Concrete.CommunicationGUID getGuid() { 
		return protoObj.getGuid();
	}

	public String getCommunicationId() {
		return protoObj.getGuid().getCommunicationId();
	}

	public String getCorpusName() {
		return protoObj.getGuid().getCorpusName();
	}

	//======================================================================
	// Vertex Accessors
	//======================================================================

	/** Return the unique vertex in this communication's knowledge
	 * graph that corresponds with the communication itself.  If there
	 * is no such vertex (or if there are multiple such vertices) then
	 * raise an exception. */
	public IndexedVertex getCommunicationVertex() throws RebarException {
		// cache this result?
		return getKnowledgeGraph().getCommunicationVertex(getCommunicationId());
	}

	/** Return the unique PERSON vertex in this communication's
	 * knowledge graph that is labeled as the sender of this
	 * communication.  If there is no such vertex (or if there are
	 * multiple such vertices) then raise an exception.  If you are
	 * not sure how many senders a communication might have, then use
	 * getSenderVertices() instead. */
	public IndexedVertex getSenderPersonVertex() throws RebarException {
		throw new RebarException("NOT IMPLEMENTED YET");
	}

	/** Return the set of PERSON vertices in this communication's
	 * knowledge graph that are labeled as the senders of this
	 * communication.  If you are sure that there will always be
	 * exactly one sender, then you can use getSenderVertex()
	 * instead.  */
	public Collection<IndexedVertex> getSenderPersonVertices() throws RebarException {
		throw new RebarException("NOT IMPLEMENTED YET");
	}

	/** Return the set of PERSON vertices in this communication's
	 * knowledge graph that are labeled as the recipients of this
	 * communication. */
	public Collection<IndexedVertex> getRecipientPersonVertices() throws RebarException {
		throw new RebarException("NOT IMPLEMENTED YET");
	}

	/** Return the unique COMM_CHANNEL vertex in this communication's
	 * knowledge graph that is labeled as the sender of this
	 * communication.  If there is no such vertex (or if there are
	 * multiple such vertices) then raise an exception.  If you are
	 * not sure how many senders a communication might have, then use
	 * getSenderVertices() instead. */
	public IndexedVertex getSenderComChannelVertex() throws RebarException {
		throw new RebarException("NOT IMPLEMENTED YET");
	}

	/** Return the set of COMM_CHANNEL vertices in this communication's
	 * knowledge graph that are labeled as the senders of this
	 * communication.  If you are sure that there will always be
	 * exactly one sender, then you can use getSenderVertex()
	 * instead.  */
	public Collection<IndexedVertex> getSenderComChannelVertices() throws RebarException {
		throw new RebarException("NOT IMPLEMENTED YET");
	}

	/** Return the set of COMM_CHANNEL vertices in this communication's
	 * knowledge graph that are labeled as the recipients of this
	 * communication. */
	public Collection<IndexedVertex> getRecipientComChannelVertices() throws RebarException {
		throw new RebarException("NOT IMPLEMENTED YET");
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
	// Token/Tokenization Index Lookup
	//======================================================================

	public Concrete.Token getToken(Concrete.TokenRef ref) throws RebarException {
		return IndexedTokenization.build(ref.getTokenization(), getIndex()).getToken(ref.getTokenId());
	}
}

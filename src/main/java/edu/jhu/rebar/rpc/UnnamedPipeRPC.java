/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

/** STILL TO DO:

 - Handling of Identifier Sets
 - getEdgesWithOneVertexIn
 - getEdgesWithBothVerticesIn

 */
package edu.jhu.rebar.rpc;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import edu.jhu.hlt.concrete.Concrete;
import edu.jhu.hlt.concrete.rpc.ConcreteRpc;
import edu.jhu.hlt.concrete.rpc.ConcreteRpc.RPCRequest;
import edu.jhu.hlt.concrete.rpc.ConcreteRpc.RPCResponse;
import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.CorpusFactory;
import edu.jhu.rebar.Graph;
import edu.jhu.rebar.IndexedCommunication;
import edu.jhu.rebar.IndexedEdge;
import edu.jhu.rebar.IndexedVertex;
import edu.jhu.rebar.ProtoIndex;
import edu.jhu.rebar.RebarBackends;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;
import edu.jhu.rebar.StageOwnership;
//import edu.jhu.hlt.concrete.rpc.*;
//import edu.jhu.hlt.concrete.rpc.ConcreteRpc.RPCResponse;
import edu.jhu.rebar.file.FileCorpusFactory;

/**
 * Remote Procedure Call (RPC) server that uses unnamed pipes (aka stdin and
 * stdout) to communicate. This class is meant to be spawned as a subprocess.
 * See rpc.proto for details about the communication protocol used.
 */
class UnnamedPipeRPC {
    private final CorpusFactory cf;

    static final int ITERATOR_BYTE_LIMIT = 10000;
    private static final Logger LOGGER = Logger.getLogger(UnnamedPipeRPC.class);
    private static final RPCResponse okResponse = RPCResponse.newBuilder().build();

    // The streams we use to communicate.
    private final InputStream inputStream;
    private final OutputStream outputStream;

    private long nextId = 0;
    private final Map<Long, Corpus> corpora;
    private final Map<Long, Corpus.Initializer> corpusInitializers;
    private final Map<Long, Corpus.Reader> corpusReaders;
    private final Map<Long, Corpus.DiffWriter> corpusDiffWriters;
    private final Map<Long, Iterator<IndexedCommunication>> communicationIterators;
    private final Map<Long, Graph> graphs;
    private final Map<Long, Graph.Initializer> graphInitializers;
    private final Map<Long, Graph.Reader> graphReaders;
    private final Map<Long, Graph.DiffWriter> graphDiffWriters;
    private final Map<Long, Iterator<IndexedVertex>> vertexIterators;
    private final Map<Long, Iterator<IndexedEdge>> edgeIterators;
    private final Map<Long, Collection<String>> comIdSets;
    private final Map<Long, Collection<Concrete.UUID>> vertexIdSets;
    private final Map<Long, Collection<Concrete.EdgeId>> edgeIdSets;

    public UnnamedPipeRPC() {
        try {
            this.cf = new FileCorpusFactory();
        } catch (RebarException re) {
            throw new RuntimeException(re);
        }
        inputStream = System.in;
        outputStream = System.out;
        corpora = new HashMap<Long, Corpus>();
        corpusInitializers = new HashMap<Long, Corpus.Initializer>();
        corpusReaders = new HashMap<Long, Corpus.Reader>();
        corpusDiffWriters = new HashMap<Long, Corpus.DiffWriter>();
        communicationIterators = new HashMap<Long, Iterator<IndexedCommunication>>();
        graphs = new HashMap<Long, Graph>();
        graphInitializers = new HashMap<Long, Graph.Initializer>();
        graphReaders = new HashMap<Long, Graph.Reader>();
        graphDiffWriters = new HashMap<Long, Graph.DiffWriter>();
        vertexIterators = new HashMap<Long, Iterator<IndexedVertex>>();
        edgeIterators = new HashMap<Long, Iterator<IndexedEdge>>();
        comIdSets = new HashMap<Long, Collection<String>>();
        vertexIdSets = new HashMap<Long, Collection<Concrete.UUID>>();
        edgeIdSets = new HashMap<Long, Collection<Concrete.EdgeId>>();
    }

    private void close() throws RebarException {
        // Close everything we own.
        for (Corpus.Initializer x : corpusInitializers.values())
            x.close();
        for (Corpus.Reader x : corpusReaders.values())
            x.close();
        for (Corpus.DiffWriter x : corpusDiffWriters.values())
            x.close();
        for (Graph.Initializer x : graphInitializers.values())
            x.close();
        for (Graph.Reader x : graphReaders.values())
            x.close();
        for (Graph.DiffWriter x : graphDiffWriters.values())
            x.close();
        for (Corpus c : corpora.values())
            c.close();
        for (Graph g : graphs.values())
            g.close();
        corpusInitializers.clear();
        corpusReaders.clear();
        corpusDiffWriters.clear();
        graphInitializers.clear();
        graphReaders.clear();
        graphDiffWriters.clear();
        corpora.clear();
        graphs.clear();
    }

    public void run() throws IOException, RebarException {
        while (true) {
            // Read a varint (length) followed by the actual message.
            RPCRequest request = RPCRequest.parseDelimitedFrom(inputStream);
            try {
                if (request == null) {
                    LOGGER.info("Stream disconnected!");
                    return;
                }
                // Process the request
                RPCResponse response = handleRequest(request);
                // Send the response back
                response.writeDelimitedTo(outputStream);
                outputStream.flush();
                // Check whether we're finished.
                if (request.getClose())
                    break;
            } catch (RebarException e) {
                final StringWriter sw = new StringWriter();
                final PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                buildErrorResponse(e.toString() + "\n" + sw.toString()).writeDelimitedTo(outputStream);
                outputStream.flush();
            }
        }
    }

    private RPCResponse handleRequest(RPCRequest request) throws RebarException {
        // Note: we intentionally put requests that we expect to be
        // more common earlier in this list. We might also replace
        // this with an enum-based switch or with a handler
        // delegation.

        // 1. Iterator access methods
        if (request.hasCommunicationIteratorNext())
            return handleCommunicationIteratorNext(request.getCommunicationIteratorNext());
        else if (request.hasVertexIteratorNext())
            return handleVertexIteratorNext(request.getVertexIteratorNext());
        else if (request.hasEdgeIteratorNext())
            return handleEdgeIteratorNext(request.getEdgeIteratorNext());

        // 2. Iterator has-next methods
        else if (request.hasCommunicationIteratorHasNext())
            return handleCommunicationIteratorHasNext(request.getCommunicationIteratorHasNext());
        else if (request.hasVertexIteratorHasNext())
            return handleVertexIteratorHasNext(request.getVertexIteratorHasNext());
        else if (request.hasEdgeIteratorHasNext())
            return handleEdgeIteratorHasNext(request.getEdgeIteratorHasNext());

        // 3. Write methods
        else if (request.hasCorpusDiffWriterWriteCommunicationDiff())
            return handleCorpusDiffWriterWriteCommunicationDiff(request.getCorpusDiffWriterWriteCommunicationDiff());
        else if (request.hasGraphDiffWriterWriteVertexDiff())
            return handleGraphDiffWriterWriteVertexDiff(request.getGraphDiffWriterWriteVertexDiff());
        else if (request.hasGraphDiffWriterWriteEdgeDiff())
            return handleGraphDiffWriterWriteEdgeDiff(request.getGraphDiffWriterWriteEdgeDiff());

        // 4. Everything else
        else if (request.hasGetCorpus())
            return handleGetCorpus(request.getGetCorpus());
        else if (request.hasMakeCorpus())
            return handleMakeCorpus(request.getMakeCorpus());
        else if (request.hasDeleteCorpus())
            return handleDeleteCorpus(request.getDeleteCorpus());
        else if (request.hasListCorpora())
            return handleListCorpora(request.getListCorpora());
        // Corpus methods
        else if (request.hasCorpusClose())
            return handleCorpusClose(request.getCorpusClose());
//        else if (request.hasCorpusGetSize())
//            return handleCorpusGetSize(request.getCorpusGetSize());
        else if (request.hasCorpusGetStages())
            return handleCorpusGetStages(request.getCorpusGetStages());
        else if (request.hasCorpusMakeStage())
            return handleCorpusMakeStage(request.getCorpusMakeStage());
        else if (request.hasCorpusDeleteStage())
            return handleCorpusDeleteStage(request.getCorpusDeleteStage());
        else if (request.hasCorpusMarkStagePublic())
            return handleCorpusMarkStagePublic(request.getCorpusMarkStagePublic());
        else if (request.hasCorpusMakeInitializer())
            return handleCorpusMakeInitializer(request.getCorpusMakeInitializer());
        else if (request.hasCorpusMakeReader())
            return handleCorpusMakeReader(request.getCorpusMakeReader());
        else if (request.hasCorpusMakeDiffWriter())
            return handleCorpusMakeDiffWriter(request.getCorpusMakeDiffWriter());
        else if (request.hasCorpusReadComIdSet())
            return handleCorpusReadComIdSet(request.getCorpusReadComIdSet());
        else if (request.hasCorpusLookupComIdSet())
            return handleCorpusLookupComIdSet(request.getCorpusLookupComIdSet());
        else if (request.hasCorpusRegisterComIdSet())
            return handleCorpusRegisterComIdSet(request.getCorpusRegisterComIdSet());
        else if (request.hasCorpusGetNumCommunications())
            return handleCorpusGetNumCommunications(request.getCorpusGetNumCommunications());
        else if (request.hasCorpusGetComIdSetNames())
            return handleCorpusGetComIdSetNames(request.getCorpusGetComIdSetNames());
        // Corpus.Initializer Methods
        else if (request.hasCorpusInitializerClose())
            return handleCorpusInitializerClose(request.getCorpusInitializerClose());
        else if (request.hasCorpusInitializerAddCommunication())
            return handleCorpusInitializerAddCommunication(request.getCorpusInitializerAddCommunication());
        // Corpus.Reader methods
        else if (request.hasCorpusReaderClose())
            return handleCorpusReaderClose(request.getCorpusReaderClose());
        else if (request.hasCorpusReaderLoadCommunication())
            return handleCorpusReaderLoadCommunication(request.getCorpusReaderLoadCommunication());
        else if (request.hasCorpusReaderLoadCommunications())
            return handleCorpusReaderLoadCommunications(request.getCorpusReaderLoadCommunications());
        // Corpus.DiffWriter methods
        else if (request.hasCorpusDiffWriterClose())
            return handleCorpusDiffWriterClose(request.getCorpusDiffWriterClose());
        else if (request.hasCorpusDiffWriterFlush())
            return handleCorpusDiffWriterFlush(request.getCorpusDiffWriterFlush());
        // Graph Methods (Static)
        else if (request.hasMakeGraph())
            return handleMakeGraph(request.getMakeGraph());
        else if (request.hasGetGraph())
            return handleGetGraph(request.getGetGraph());
        else if (request.hasDeleteGraph())
            return handleDeleteGraph(request.getDeleteGraph());
        else if (request.hasListGraphs())
            return handleListGraphs(request.getListGraphs());
        // Graph Methods
        else if (request.hasGraphClose())
            return handleGraphClose(request.getGraphClose());
        else if (request.hasGraphGetStages())
            return handleGraphGetStages(request.getGraphGetStages());
        else if (request.hasGraphMakeStage())
            return handleGraphMakeStage(request.getGraphMakeStage());
        else if (request.hasGraphDeleteStage())
            return handleGraphDeleteStage(request.getGraphDeleteStage());
        else if (request.hasGraphMarkStagePublic())
            return handleGraphMarkStagePublic(request.getGraphMarkStagePublic());
        else if (request.hasGraphMakeInitializer())
            return handleGraphMakeInitializer(request.getGraphMakeInitializer());
        else if (request.hasGraphMakeReader())
            return handleGraphMakeReader(request.getGraphMakeReader());
        else if (request.hasGraphMakeDiffWriter())
            return handleGraphMakeDiffWriter(request.getGraphMakeDiffWriter());
        else if (request.hasGraphGetEdgesWithOneVertexIn())
            return handleGraphGetEdgesWithOneVertexIn(request.getGraphGetEdgesWithOneVertexIn());
        else if (request.hasGraphGetEdgesWithBothVerticesIn())
            return handleGraphGetEdgesWithBothVerticesIn(request.getGraphGetEdgesWithBothVerticesIn());
        else if (request.hasReadVertexIdSet())
            return handleReadVertexIdSet(request.getReadVertexIdSet());
        else if (request.hasLookupVertexIdSet())
            return handleLookupVertexIdSet(request.getLookupVertexIdSet());
        else if (request.hasRegisterVertexIdSet())
            return handleRegisterVertexIdSet(request.getRegisterVertexIdSet());
        else if (request.hasGetVertexIdSetNames())
            return handleGetVertexIdSetNames(request.getGetVertexIdSetNames());
        else if (request.hasReadEdgeIdSet())
            return handleReadEdgeIdSet(request.getReadEdgeIdSet());
        else if (request.hasLookupEdgeIdSet())
            return handleLookupEdgeIdSet(request.getLookupEdgeIdSet());
        else if (request.hasRegisterEdgeIdSet())
            return handleRegisterEdgeIdSet(request.getRegisterEdgeIdSet());
        else if (request.hasGetEdgeIdSetNames())
            return handleGetEdgeIdSetNames(request.getGetEdgeIdSetNames());
        else if (request.hasGraphGetNumVertices())
            return handleGraphGetNumVertices(request.getGraphGetNumVertices());
//        else if (request.hasGraphGetSize())
//            return handleGraphGetSize(request.getGraphGetSize());
        // Graph.Initializer Methods
        else if (request.hasGraphInitializerClose())
            return handleGraphInitializerClose(request.getGraphInitializerClose());
        else if (request.hasGraphInitializerAddVertex())
            return handleGraphInitializerAddVertex(request.getGraphInitializerAddVertex());
        // Graph.Reader Methods
        else if (request.hasGraphReaderClose())
            return handleGraphReaderClose(request.getGraphReaderClose());
        else if (request.hasGraphReaderLoadVertex())
            return handleGraphReaderLoadVertex(request.getGraphReaderLoadVertex());
        else if (request.hasGraphReaderLoadVertices())
            return handleGraphReaderLoadVertices(request.getGraphReaderLoadVertices());
        else if (request.hasGraphReaderLoadEdge())
            return handleGraphReaderLoadEdge(request.getGraphReaderLoadEdge());
        else if (request.hasGraphReaderLoadEdges())
            return handleGraphReaderLoadEdges(request.getGraphReaderLoadEdges());
        // Graph.DiffWriter Methods
        else if (request.hasGraphDiffWriterClose())
            return handleGraphDiffWriterClose(request.getGraphDiffWriterClose());
        else if (request.hasGraphDiffWriterFlush())
            return handleGraphDiffWriterFlush(request.getGraphDiffWriterFlush());
        // IdSet Methods
        else if (request.hasComIdSetEnumerate())
            return handleComIdSetEnumerate(request.getComIdSetEnumerate());
        else if (request.hasVertexIdSetEnumerate())
            return handleVertexIdSetEnumerate(request.getVertexIdSetEnumerate());
        else if (request.hasEdgeIdSetEnumerate())
            return handleEdgeIdSetEnumerate(request.getEdgeIdSetEnumerate());
        // Close
        else if (request.hasClose())
            return okResponse;
        else
            throw new RebarException("Unexpected request type");
    }

    /**
     * Assign an identifier to an object, and store it in an index using that
     * identifier.
     */
    private <T> long assignId(T obj, Map<Long, T> index) {
        long id = nextId++;
        index.put(id, obj);
        return id;
    }

    // ////////////////////////////////////////////////////////////////////
    // Parse Helpers
    // ////////////////////////////////////////////////////////////////////

    private Map<ProtoIndex.ModificationTarget, byte[]> parseDiff(ConcreteRpc.ProtoDiff diff) {
        final Map<ProtoIndex.ModificationTarget, byte[]> result = new HashMap<ProtoIndex.ModificationTarget, byte[]>();
        for (ConcreteRpc.ProtoDiff.Entry entry : diff.getDiffList()) {
            ProtoIndex.ModificationTarget target;
            if (entry.hasUuid())
                target = new ProtoIndex.ModificationTarget(entry.getUuid());
            else
                target = new ProtoIndex.ModificationTarget(entry.getEdgeId());
            result.put(target, entry.getProto().toByteArray());
        }
        return result;
    }

    private Collection<String> parseComIdCollection(Corpus corpus, ConcreteRpc.ComIdCollection comList) throws RebarException {
        if (comList.hasComIdSet())
            return comIdSets.get(comList.hasComIdSet());
        else if (comList.hasComIdSetName())
            return corpus.lookupComIdSet(comList.getComIdSetName());
        else
            return comList.getComIdList();
    }

    private Collection<Concrete.UUID> parseVertexIdCollection(Graph graph, ConcreteRpc.VertexIdCollection vertexList) throws RebarException {
        if (vertexList.hasVertexIdSet())
            return vertexIdSets.get(vertexList.hasVertexIdSet());
        else if (vertexList.hasVertexIdSetName())
            return graph.lookupVertexIdSet(vertexList.getVertexIdSetName());
        else {
            return vertexList.getVertexIdList();
        }
    }

    private Collection<Concrete.EdgeId> parseEdgeIdCollection(Graph graph, ConcreteRpc.EdgeIdCollection edgeList) throws RebarException {
        if (edgeList.hasEdgeIdSet())
            return edgeIdSets.get(edgeList.hasEdgeIdSet());
        else if (edgeList.hasEdgeIdSetName())
            return graph.lookupEdgeIdSet(edgeList.getEdgeIdSetName());
        else {
            return edgeList.getEdgeIdList();
        }
    }

    // ////////////////////////////////////////////////////////////////////
    // Response Generator Helpers: Error Messages
    // ////////////////////////////////////////////////////////////////////

    private RPCResponse buildErrorResponse(String errMsg) {
        return RPCResponse.newBuilder().setError(errMsg).build();
    }

    // ////////////////////////////////////////////////////////////////////
    // Response Generator Helpers: Corpus
    // ////////////////////////////////////////////////////////////////////

    private ConcreteRpc.Corpus buildRpcCorpus(long id) {
        return ConcreteRpc.Corpus.newBuilder().setId(id).build();
    }

    private ConcreteRpc.CorpusInitializer buildRpcCorpusInitializer(ConcreteRpc.Corpus owner, long id) {
        return ConcreteRpc.CorpusInitializer.newBuilder().setId(id).build();
    }

    private ConcreteRpc.CorpusReader buildRpcCorpusReader(ConcreteRpc.Corpus owner, long id) {
        return ConcreteRpc.CorpusReader.newBuilder().setId(id).build();
    }

    private ConcreteRpc.CorpusDiffWriter buildRpcCorpusDiffWriter(ConcreteRpc.Corpus owner, long id) {
        return ConcreteRpc.CorpusDiffWriter.newBuilder().setId(id).build();
    }

    private ConcreteRpc.CommunicationIterator buildRpcCommunicationIterator(ConcreteRpc.CorpusReader owner, long id) {
        return ConcreteRpc.CommunicationIterator.newBuilder().setId(id).build();
    }

    private ConcreteRpc.ComIdSet buildRpcComIdSet(ConcreteRpc.Corpus owner, long id) {
        return ConcreteRpc.ComIdSet.newBuilder().setId(id).build();
    }

    // ////////////////////////////////////////////////////////////////////
    // Response Generator Helpers: Graph
    // ////////////////////////////////////////////////////////////////////

    private ConcreteRpc.Graph buildRpcGraph(long id) {
        return ConcreteRpc.Graph.newBuilder().setId(id).build();
    }

    private ConcreteRpc.GraphInitializer buildRpcGraphInitializer(ConcreteRpc.Graph owner, long id) {
        return ConcreteRpc.GraphInitializer.newBuilder().setId(id).build();
    }

    private ConcreteRpc.GraphReader buildRpcGraphReader(ConcreteRpc.Graph owner, long id) {
        return ConcreteRpc.GraphReader.newBuilder().setId(id).build();
    }

    private ConcreteRpc.GraphDiffWriter buildRpcGraphDiffWriter(ConcreteRpc.Graph owner, long id) {
        return ConcreteRpc.GraphDiffWriter.newBuilder().setId(id).build();
    }

    private ConcreteRpc.VertexIterator buildRpcVertexIterator(ConcreteRpc.GraphReader owner, long id) {
        return ConcreteRpc.VertexIterator.newBuilder().setId(id).build();
    }

    private ConcreteRpc.EdgeIterator buildRpcEdgeIterator(ConcreteRpc.GraphReader owner, long id) {
        return ConcreteRpc.EdgeIterator.newBuilder().setId(id).build();
    }

    private ConcreteRpc.VertexIdSet buildRpcVertexIdSet(ConcreteRpc.Graph owner, long id) {
        return ConcreteRpc.VertexIdSet.newBuilder().setId(id).build();
    }

    private ConcreteRpc.EdgeIdSet buildRpcEdgeIdSet(ConcreteRpc.Graph owner, long id) {
        return ConcreteRpc.EdgeIdSet.newBuilder().setId(id).build();
    }

    // ////////////////////////////////////////////////////////////////////
    // Response Generator Helpers: Stage
    // ////////////////////////////////////////////////////////////////////

    private ConcreteRpc.Stage buildRpcStage(Stage stage) {
        List<Integer> dependencies = new ArrayList<Integer>();
        for (Stage dep : stage.getDependencies())
            dependencies.add(dep.getStageId());
        return ConcreteRpc.Stage.newBuilder().setStageName(stage.getStageName()).setStageVersion(stage.getStageVersion())
                .setStageId(stage.getStageId()).addAllDependency(dependencies).setDescription(stage.getDescription())
                .setIsPublic(stage.isPublic()).build();
    }

    private void addStageOwnership(StageOwnership stageOwnership, RPCResponse.Builder response) {
        if (stageOwnership == null)
            return;
        ConcreteRpc.StageOwnershipMap.Builder ownershipMap = response.addStageOwnershipBuilder();
        for (Map.Entry<StageOwnership.FieldValuePointer, Stage> e : stageOwnership.entrySet()) {
            StageOwnership.FieldValuePointer fvp = e.getKey();
            List<ConcreteRpc.StageOwnershipMap.Segment> segments = new ArrayList<ConcreteRpc.StageOwnershipMap.Segment>();
            while (fvp != null) {
                segments.add(ConcreteRpc.StageOwnershipMap.Segment.newBuilder().setFieldTag(fvp.field.getNumber()).setIndex(fvp.index)
                        .build());
                fvp = fvp.parent;
            }
            Collections.reverse(segments);
            ownershipMap.addEntryBuilder().setStageId(e.getValue().getStageId()).addAllPathSegment(segments);
        }
    }

    // ////////////////////////////////////////////////////////////////////
    // Request Handlers: Corpus
    // ////////////////////////////////////////////////////////////////////

    private RPCResponse handleGetCorpus(ConcreteRpc.GetCorpus request) throws RebarException {
        long id = assignId(this.cf.getCorpus(request.getName()), corpora);
        return RPCResponse.newBuilder().setCorpus(buildRpcCorpus(id)).build();
    }

    private RPCResponse handleMakeCorpus(ConcreteRpc.MakeCorpus request) throws RebarException {
        long id = assignId(this.cf.makeCorpus(request.getName()), corpora);
        return RPCResponse.newBuilder().setCorpus(buildRpcCorpus(id)).build();
    }

    private RPCResponse handleDeleteCorpus(ConcreteRpc.DeleteCorpus request) throws RebarException {
        this.cf.deleteCorpus(request.getName());
        return okResponse;
    }

    private RPCResponse handleListCorpora(ConcreteRpc.ListCorpora request) throws RebarException {
        return RPCResponse.newBuilder().addAllCorpusName(this.cf.listCorpora()).build();
    }

    private RPCResponse handleCorpusClose(ConcreteRpc.Corpus.Close request) throws RebarException {
        Corpus self = corpora.get(request.getSelf().getId());
        self.close();
        return okResponse;
    }

//    private RPCResponse handleCorpusGetSize(ConcreteRpc.Corpus.GetSize request) throws RebarException {
//        Corpus self = corpora.get(request.getSelf().getId());
//        long size = self.getSize();
//        return RPCResponse.newBuilder().setSize(size).build();
//    }

    private RPCResponse handleCorpusGetStages(ConcreteRpc.Corpus.GetStages request) throws RebarException {
        Corpus self = corpora.get(request.getSelf().getId());
        RPCResponse.Builder builder = RPCResponse.newBuilder();
        for (Stage stage : self.getStages())
            builder.addStage(buildRpcStage(stage));
        return builder.build();
    }

    private RPCResponse handleCorpusMakeStage(ConcreteRpc.Corpus.MakeStage request) throws RebarException {
        Corpus self = corpora.get(request.getSelf().getId());
        Set<Stage> dependencies = new TreeSet<Stage>();
        for (int dep : request.getDependencyList())
            dependencies.add(self.getStage(dep));
        Stage newStage = self.makeStage(request.getStageName(), request.getStageVersion(), dependencies, request.getDescription(),
                request.getDeleteIfExists());
        return RPCResponse.newBuilder().addStage(buildRpcStage(newStage)).build();
    }

    private RPCResponse handleCorpusDeleteStage(ConcreteRpc.Corpus.DeleteStage request) throws RebarException {
        Corpus self = corpora.get(request.getSelf().getId());
        self.deleteStage(self.getStage(request.getStageId()));
        return okResponse;
    }

    private RPCResponse handleCorpusMarkStagePublic(ConcreteRpc.Corpus.MarkStagePublic request) throws RebarException {
        Corpus self = corpora.get(request.getSelf().getId());
        self.markStagePublic(self.getStage(request.getStageId()));
        return okResponse;
    }

    private RPCResponse handleCorpusMakeInitializer(ConcreteRpc.Corpus.MakeInitializer request) throws RebarException {
        Corpus self = corpora.get(request.getSelf().getId());
        long id = assignId(self.makeInitializer(), corpusInitializers);
        return RPCResponse.newBuilder().setCorpusInitializer(buildRpcCorpusInitializer(request.getSelf(), id)).build();
    }

    private RPCResponse handleCorpusMakeReader(ConcreteRpc.Corpus.MakeReader request) throws RebarException {
        Corpus self = corpora.get(request.getSelf().getId());
        Collection<Stage> dependencies = new ArrayList<Stage>();
        for (int dep : request.getDependencyList())
            dependencies.add(self.getStage(dep));
        boolean loadStageOwnership = request.getLoadStageOwnership();
        long id = assignId(self.makeReader(dependencies, loadStageOwnership), corpusReaders);
        return RPCResponse.newBuilder().setCorpusReader(buildRpcCorpusReader(request.getSelf(), id)).build();
    }

    private RPCResponse handleCorpusMakeDiffWriter(ConcreteRpc.Corpus.MakeDiffWriter request) throws RebarException {
        Corpus self = corpora.get(request.getSelf().getId());
        Stage stage = self.getStage(request.getStageId());
        long id = assignId(self.makeDiffWriter(stage), corpusDiffWriters);
        return RPCResponse.newBuilder().setCorpusDiffWriter(buildRpcCorpusDiffWriter(request.getSelf(), id)).build();
    }

    private RPCResponse handleCorpusReadComIdSet(ConcreteRpc.Corpus.ReadComIdSet request) throws RebarException {
        Corpus self = corpora.get(request.getSelf().getId());
        Collection<String> comIdSet = self.readComIdSet(new File(request.getFilename()));
        long id = assignId(comIdSet, comIdSets);
        return RPCResponse.newBuilder().setComIdSet(buildRpcComIdSet(request.getSelf(), id)).build();
    }

    private RPCResponse handleCorpusLookupComIdSet(ConcreteRpc.Corpus.LookupComIdSet request) throws RebarException {
        Corpus self = corpora.get(request.getSelf().getId());
        Collection<String> comIdSet = self.lookupComIdSet(request.getName());
        long id = assignId(comIdSet, comIdSets);
        return RPCResponse.newBuilder().setComIdSet(buildRpcComIdSet(request.getSelf(), id)).build();
    }

    private RPCResponse handleCorpusRegisterComIdSet(ConcreteRpc.Corpus.RegisterComIdSet request) throws RebarException {
        Corpus self = corpora.get(request.getSelf().getId());
        Collection<String> comIds = parseComIdCollection(self, request.getComIds());
        self.registerComIdSet(request.getName(), comIds);
        return okResponse;
    }

    private RPCResponse handleCorpusGetComIdSetNames(ConcreteRpc.Corpus.GetComIdSetNames request) throws RebarException {
        Corpus self = corpora.get(request.getSelf().getId());
        Collection<String> names = self.getComIdSetNames();
        RPCResponse.Builder responseBuilder = RPCResponse.newBuilder();
        for (String name : names)
            responseBuilder.addIdSetName(name);
        return responseBuilder.build();
    }

    private RPCResponse handleCorpusGetNumCommunications(ConcreteRpc.Corpus.GetNumCommunications request) throws RebarException {
        Corpus self = corpora.get(request.getSelf().getId());
        long size = self.getNumCommunications();
        return RPCResponse.newBuilder().setSize(size).build();
    }

    // ////////////////////////////////////////////////////////////////////
    // Request Handlers: Corpus.Initializer
    // ////////////////////////////////////////////////////////////////////

    private RPCResponse handleCorpusInitializerClose(ConcreteRpc.CorpusInitializer.Close request) throws RebarException {
        Corpus.Initializer self = corpusInitializers.get(request.getSelf().getId());
        self.close();
        return okResponse;
    }

    private RPCResponse handleCorpusInitializerAddCommunication(ConcreteRpc.CorpusInitializer.AddCommunication request)
            throws RebarException {
        Corpus.Initializer self = corpusInitializers.get(request.getSelf().getId());
        self.addCommunication(request.getCommunication());
        return okResponse;
    }

    // ////////////////////////////////////////////////////////////////////
    // Request Handlers: Corpus.Reader
    // ////////////////////////////////////////////////////////////////////

    private RPCResponse handleCorpusReaderClose(ConcreteRpc.CorpusReader.Close request) throws RebarException {
        Corpus.Reader self = corpusReaders.get(request.getSelf().getId());
        self.close();
        return okResponse;
    }

    private RPCResponse handleCorpusReaderLoadCommunication(ConcreteRpc.CorpusReader.LoadCommunication request) throws RebarException {
        final Corpus.Reader self = corpusReaders.get(request.getSelf().getId());
        final IndexedCommunication com = self.loadCommunication(request.getComId());
        RPCResponse.Builder response = RPCResponse.newBuilder().addCommunication(com.getProto());
        addStageOwnership(com.getStageOwnership(), response);
        return response.build();
    }

    private RPCResponse handleCorpusReaderLoadCommunications(ConcreteRpc.CorpusReader.LoadCommunications request) throws RebarException {
        final Corpus.Reader self = corpusReaders.get(request.getSelf().getId());
        final Collection<String> comIds = parseComIdCollection(self.getCorpus(), request.getComIds());
        final Iterator<IndexedCommunication> iter = self.loadCommunications(comIds);
        final long id = assignId(iter, communicationIterators);
        return RPCResponse.newBuilder().setCommunicationIterator(buildRpcCommunicationIterator(request.getSelf(), id)).build();
    }

    // ////////////////////////////////////////////////////////////////////
    // Request Handlers: Corpus.DiffWriter
    // ////////////////////////////////////////////////////////////////////

    private RPCResponse handleCorpusDiffWriterClose(ConcreteRpc.CorpusDiffWriter.Close request) throws RebarException {
        Corpus.DiffWriter self = corpusDiffWriters.get(request.getSelf().getId());
        if (self == null)
            throw new RebarException("CorpusDiffWriter not found!");
        self.close();
        return okResponse;
    }

    private RPCResponse handleCorpusDiffWriterFlush(ConcreteRpc.CorpusDiffWriter.Flush request) throws RebarException {
        Corpus.DiffWriter self = corpusDiffWriters.get(request.getSelf().getId());
        if (self == null)
            throw new RebarException("CorpusDiffWriter not found!");
        self.flush();
        return okResponse;
    }

    private RPCResponse handleCorpusDiffWriterWriteCommunicationDiff(ConcreteRpc.CorpusDiffWriter.WriteCommunicationDiff request)
            throws RebarException {
        Corpus.DiffWriter self = corpusDiffWriters.get(request.getSelf().getId());
        if (self == null)
            throw new RebarException("CorpusDiffWriter not found!");
        self.saveCommunicationDiff(request.getComId(), parseDiff(request.getDiff()));
        return okResponse;
    }

    // ////////////////////////////////////////////////////////////////////
    // Request Handlers: Iterator<IndexedCommunication>
    // ////////////////////////////////////////////////////////////////////

    private RPCResponse handleCommunicationIteratorHasNext(ConcreteRpc.CommunicationIterator.HasNext request) throws RebarException {
        Iterator<IndexedCommunication> self = communicationIterators.get(request.getSelf().getId());
        return RPCResponse.newBuilder().setHasNext(self.hasNext()).build();
    }

    private RPCResponse handleCommunicationIteratorNext(ConcreteRpc.CommunicationIterator.Next request) throws RebarException {
        Iterator<IndexedCommunication> self = communicationIterators.get(request.getSelf().getId());
        int maxValues = request.getMaxValues();
        int bytes = 0;
        RPCResponse.Builder response = RPCResponse.newBuilder();
        while (self.hasNext() && (maxValues-- > 0) && (bytes < ITERATOR_BYTE_LIMIT)) {
            IndexedCommunication com = self.next();
            response.addCommunication(com.getProto());
            addStageOwnership(com.getStageOwnership(), response);
            bytes += com.getProto().getSerializedSize();
        }
        return response.build();
    }

    // ////////////////////////////////////////////////////////////////////
    // Request Handlers: Graph
    // ////////////////////////////////////////////////////////////////////

    private RPCResponse handleGetGraph(ConcreteRpc.GetGraph request) throws RebarException {
        long id = assignId(Graph.Factory.getGraph(request.getName()), graphs);
        return RPCResponse.newBuilder().setGraph(buildRpcGraph(id)).build();
    }

    private RPCResponse handleMakeGraph(ConcreteRpc.MakeGraph request) throws RebarException {
        long id = assignId(Graph.Factory.makeGraph(request.getName()), graphs);
        return RPCResponse.newBuilder().setGraph(buildRpcGraph(id)).build();
    }

    private RPCResponse handleDeleteGraph(ConcreteRpc.DeleteGraph request) throws RebarException {
        Graph.Factory.deleteGraph(request.getName());
        return okResponse;
    }

    private RPCResponse handleListGraphs(ConcreteRpc.ListGraphs request) throws RebarException {
        return RPCResponse.newBuilder().addAllGraphName(Graph.Factory.list()).build();
    }

    private RPCResponse handleGraphClose(ConcreteRpc.Graph.Close request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        self.close();
        return okResponse;
    }

    private RPCResponse handleGraphGetStages(ConcreteRpc.Graph.GetStages request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        RPCResponse.Builder builder = RPCResponse.newBuilder();
        for (Stage stage : self.getStages())
            builder.addStage(buildRpcStage(stage));
        return builder.build();
    }

    private RPCResponse handleGraphMakeStage(ConcreteRpc.Graph.MakeStage request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        Set<Stage> dependencies = new TreeSet<Stage>();
        for (int dep : request.getDependencyList())
            dependencies.add(self.getStage(dep));
        Stage newStage = self.makeStage(request.getStageName(), request.getStageVersion(), dependencies, request.getDescription(),
                request.getDeleteIfExists());
        return RPCResponse.newBuilder().addStage(buildRpcStage(newStage)).build();
    }

    private RPCResponse handleGraphDeleteStage(ConcreteRpc.Graph.DeleteStage request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        self.deleteStage(self.getStage(request.getStageId()));
        return okResponse;
    }

    private RPCResponse handleGraphMarkStagePublic(ConcreteRpc.Graph.MarkStagePublic request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        self.markStagePublic(self.getStage(request.getStageId()));
        return okResponse;
    }

    private RPCResponse handleGraphMakeInitializer(ConcreteRpc.Graph.MakeInitializer request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        long id = assignId(self.makeInitializer(), graphInitializers);
        return RPCResponse.newBuilder().setGraphInitializer(buildRpcGraphInitializer(request.getSelf(), id)).build();
    }

    private RPCResponse handleGraphMakeReader(ConcreteRpc.Graph.MakeReader request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        Collection<Stage> dependencies = new ArrayList<Stage>();
        for (int dep : request.getDependencyList())
            dependencies.add(self.getStage(dep));
        boolean loadStageOwnership = request.getLoadStageOwnership();
        long id = assignId(self.makeReader(dependencies, loadStageOwnership), graphReaders);
        return RPCResponse.newBuilder().setGraphReader(buildRpcGraphReader(request.getSelf(), id)).build();
    }

    private RPCResponse handleGraphMakeDiffWriter(ConcreteRpc.Graph.MakeDiffWriter request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        Stage stage = self.getStage(request.getStageId());
        long id = assignId(self.makeDiffWriter(stage), graphDiffWriters);
        return RPCResponse.newBuilder().setGraphDiffWriter(buildRpcGraphDiffWriter(request.getSelf(), id)).build();
    }

    private RPCResponse handleReadVertexIdSet(ConcreteRpc.Graph.ReadVertexIdSet request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        Collection<Concrete.UUID> vertexIdSet = self.readVertexIdSet(new File(request.getFilename()));
        long id = assignId(vertexIdSet, vertexIdSets);
        return RPCResponse.newBuilder().setVertexIdSet(buildRpcVertexIdSet(request.getSelf(), id)).build();
    }

    private RPCResponse handleLookupVertexIdSet(ConcreteRpc.Graph.LookupVertexIdSet request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        Collection<Concrete.UUID> vertexIdSet = self.lookupVertexIdSet(request.getName());
        long id = assignId(vertexIdSet, vertexIdSets);
        return RPCResponse.newBuilder().setVertexIdSet(buildRpcVertexIdSet(request.getSelf(), id)).build();
    }

    private RPCResponse handleRegisterVertexIdSet(ConcreteRpc.Graph.RegisterVertexIdSet request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        Collection<Concrete.UUID> vertexIds = parseVertexIdCollection(self, request.getVertexIds());
        self.registerVertexIdSet(request.getName(), vertexIds);
        return okResponse;
    }

    private RPCResponse handleGetVertexIdSetNames(ConcreteRpc.Graph.GetVertexIdSetNames request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        Collection<String> names = self.getVertexIdSetNames();
        RPCResponse.Builder responseBuilder = RPCResponse.newBuilder();
        for (String name : names)
            responseBuilder.addIdSetName(name);
        return responseBuilder.build();
    }

    private RPCResponse handleReadEdgeIdSet(ConcreteRpc.Graph.ReadEdgeIdSet request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        Collection<Concrete.EdgeId> edgeIdSet = self.readEdgeIdSet(new File(request.getFilename()));
        long id = assignId(edgeIdSet, edgeIdSets);
        return RPCResponse.newBuilder().setEdgeIdSet(buildRpcEdgeIdSet(request.getSelf(), id)).build();
    }

    private RPCResponse handleLookupEdgeIdSet(ConcreteRpc.Graph.LookupEdgeIdSet request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        Collection<Concrete.EdgeId> edgeIdSet = self.lookupEdgeIdSet(request.getName());
        long id = assignId(edgeIdSet, edgeIdSets);
        return RPCResponse.newBuilder().setEdgeIdSet(buildRpcEdgeIdSet(request.getSelf(), id)).build();
    }

    private RPCResponse handleRegisterEdgeIdSet(ConcreteRpc.Graph.RegisterEdgeIdSet request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        Collection<Concrete.EdgeId> edgeIds = parseEdgeIdCollection(self, request.getEdgeIds());
        self.registerEdgeIdSet(request.getName(), edgeIds);
        return okResponse;
    }

    private RPCResponse handleGetEdgeIdSetNames(ConcreteRpc.Graph.GetEdgeIdSetNames request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        Collection<String> names = self.getEdgeIdSetNames();
        RPCResponse.Builder responseBuilder = RPCResponse.newBuilder();
        for (String name : names)
            responseBuilder.addIdSetName(name);
        return responseBuilder.build();
    }

    private RPCResponse handleGraphGetEdgesWithOneVertexIn(ConcreteRpc.Graph.GetEdgesWithOneVertexIn request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        Collection<Concrete.UUID> vertexIds = parseVertexIdCollection(self, request.getVertexIds());
        Collection<Concrete.EdgeId> edgeIdSet = self.getEdgesWithOneVertexIn(vertexIds);
        long id = assignId(edgeIdSet, edgeIdSets);
        return RPCResponse.newBuilder().setEdgeIdSet(buildRpcEdgeIdSet(request.getSelf(), id)).build();
    }

    private RPCResponse handleGraphGetEdgesWithBothVerticesIn(ConcreteRpc.Graph.GetEdgesWithBothVerticesIn request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        Collection<Concrete.UUID> vertexIds = parseVertexIdCollection(self, request.getVertexIds());
        Collection<Concrete.EdgeId> edgeIdSet = self.getEdgesWithBothVerticesIn(vertexIds);
        long id = assignId(edgeIdSet, edgeIdSets);
        return RPCResponse.newBuilder().setEdgeIdSet(buildRpcEdgeIdSet(request.getSelf(), id)).build();
    }

    private RPCResponse handleGraphGetNumVertices(ConcreteRpc.Graph.GetNumVertices request) throws RebarException {
        Graph self = graphs.get(request.getSelf().getId());
        long size = self.getNumVertices();
        return RPCResponse.newBuilder().setSize(size).build();
    }

    // ////////////////////////////////////////////////////////////////////
    // Request Handlers: Graph.Initializer
    // ////////////////////////////////////////////////////////////////////

    private RPCResponse handleGraphInitializerClose(ConcreteRpc.GraphInitializer.Close request) throws RebarException {
        Graph.Initializer self = graphInitializers.get(request.getSelf().getId());
        self.close();
        return okResponse;
    }

    private RPCResponse handleGraphInitializerAddVertex(ConcreteRpc.GraphInitializer.AddVertex request) throws RebarException {
        Graph.Initializer self = graphInitializers.get(request.getSelf().getId());
        self.addVertex(request.getVertex());
        return okResponse;
    }

//    private RPCResponse handleGraphGetSize(ConcreteRpc.Graph.GetSize request) throws RebarException {
//        Graph self = graphs.get(request.getSelf().getId());
//        long size = self.getSize();
//        return RPCResponse.newBuilder().setSize(size).build();
//    }

    // ////////////////////////////////////////////////////////////////////
    // Request Handlers: Graph.Reader
    // ////////////////////////////////////////////////////////////////////

    private RPCResponse handleGraphReaderClose(ConcreteRpc.GraphReader.Close request) throws RebarException {
        Graph.Reader self = graphReaders.get(request.getSelf().getId());
        self.close();
        return okResponse;
    }

    private RPCResponse handleGraphReaderLoadVertex(ConcreteRpc.GraphReader.LoadVertex request) throws RebarException {
        Graph.Reader self = graphReaders.get(request.getSelf().getId());
        IndexedVertex vertex = self.loadVertex(request.getVertexId());
        RPCResponse.Builder response = RPCResponse.newBuilder().addVertex(vertex.getProto());
        addStageOwnership(vertex.getStageOwnership(), response);
        return response.build();
    }

    private RPCResponse handleGraphReaderLoadVertices(ConcreteRpc.GraphReader.LoadVertices request) throws RebarException {
        Graph.Reader self = graphReaders.get(request.getSelf().getId());
        Collection<Concrete.UUID> vertexIds = parseVertexIdCollection(self.getGraph(), request.getVertexIds());
        Iterator<IndexedVertex> iter = self.loadVertices(vertexIds);
        long id = assignId(iter, vertexIterators);
        return RPCResponse.newBuilder().setVertexIterator(buildRpcVertexIterator(request.getSelf(), id)).build();
    }

    private RPCResponse handleGraphReaderLoadEdge(ConcreteRpc.GraphReader.LoadEdge request) throws RebarException {
        Graph.Reader self = graphReaders.get(request.getSelf().getId());
        IndexedEdge edge = self.loadEdge(request.getEdgeId());
        RPCResponse.Builder response = RPCResponse.newBuilder().addEdge(edge.getProto());
        addStageOwnership(edge.getStageOwnership(), response);
        return response.build();
    }

    private RPCResponse handleGraphReaderLoadEdges(ConcreteRpc.GraphReader.LoadEdges request) throws RebarException {
        Graph.Reader self = graphReaders.get(request.getSelf().getId());
        Collection<Concrete.EdgeId> edgeIds = parseEdgeIdCollection(self.getGraph(), request.getEdgeIds());
        Iterator<IndexedEdge> iter = self.loadEdges(edgeIds);
        long id = assignId(iter, edgeIterators);
        return RPCResponse.newBuilder().setEdgeIterator(buildRpcEdgeIterator(request.getSelf(), id)).build();
    }

    // ////////////////////////////////////////////////////////////////////
    // Request Handlers: Graph.DiffWriter
    // ////////////////////////////////////////////////////////////////////

    private RPCResponse handleGraphDiffWriterClose(ConcreteRpc.GraphDiffWriter.Close request) throws RebarException {
        Graph.DiffWriter self = graphDiffWriters.get(request.getSelf().getId());
        self.close();
        return okResponse;
    }

    private RPCResponse handleGraphDiffWriterFlush(ConcreteRpc.GraphDiffWriter.Flush request) throws RebarException {
        Graph.DiffWriter self = graphDiffWriters.get(request.getSelf().getId());
        self.flush();
        return okResponse;
    }

    private RPCResponse handleGraphDiffWriterWriteVertexDiff(ConcreteRpc.GraphDiffWriter.WriteVertexDiff request) throws RebarException {
        Graph.DiffWriter self = graphDiffWriters.get(request.getSelf().getId());
        self.saveVertexDiff(request.getVertexId(), parseDiff(request.getDiff()));
        return okResponse;
    }

    private RPCResponse handleGraphDiffWriterWriteEdgeDiff(ConcreteRpc.GraphDiffWriter.WriteEdgeDiff request) throws RebarException {
        Graph.DiffWriter self = graphDiffWriters.get(request.getSelf().getId());
        self.saveEdgeDiff(request.getEdgeId(), parseDiff(request.getDiff()));
        return okResponse;
    }

    // ////////////////////////////////////////////////////////////////////
    // Request Handlers: Iterator<IndexedVertex>
    // ////////////////////////////////////////////////////////////////////

    private RPCResponse handleVertexIteratorHasNext(ConcreteRpc.VertexIterator.HasNext request) throws RebarException {
        Iterator<IndexedVertex> self = vertexIterators.get(request.getSelf().getId());
        return RPCResponse.newBuilder().setHasNext(self.hasNext()).build();
    }

    private RPCResponse handleVertexIteratorNext(ConcreteRpc.VertexIterator.Next request) throws RebarException {
        Iterator<IndexedVertex> self = vertexIterators.get(request.getSelf().getId());
        int maxValues = request.getMaxValues();
        int bytes = 0;
        RPCResponse.Builder response = RPCResponse.newBuilder();
        while (self.hasNext() && (maxValues > 0) && (bytes < ITERATOR_BYTE_LIMIT)) {
            IndexedVertex vertex = self.next();
            response.addVertex(vertex.getProto());
            addStageOwnership(vertex.getStageOwnership(), response);
            bytes += vertex.getProto().getSerializedSize();
        }
        return response.build();
    }

    // ////////////////////////////////////////////////////////////////////
    // Request Handlers: Iterator<IndexedEdge>
    // ////////////////////////////////////////////////////////////////////

    private RPCResponse handleEdgeIteratorHasNext(ConcreteRpc.EdgeIterator.HasNext request) throws RebarException {
        Iterator<IndexedEdge> self = edgeIterators.get(request.getSelf().getId());
        return RPCResponse.newBuilder().setHasNext(self.hasNext()).build();
    }

    private RPCResponse handleEdgeIteratorNext(ConcreteRpc.EdgeIterator.Next request) throws RebarException {
        Iterator<IndexedEdge> self = edgeIterators.get(request.getSelf().getId());
        int maxValues = request.getMaxValues();
        int bytes = 0;
        RPCResponse.Builder response = RPCResponse.newBuilder();
        while (self.hasNext() && (maxValues > 0) && (bytes < ITERATOR_BYTE_LIMIT)) {
            IndexedEdge edge = self.next();
            response.addEdge(edge.getProto());
            addStageOwnership(edge.getStageOwnership(), response);
            bytes += edge.getProto().getSerializedSize();
        }
        return response.build();
    }

    // ////////////////////////////////////////////////////////////////////
    // Request Handlers: IdSets
    // ////////////////////////////////////////////////////////////////////

    private RPCResponse handleComIdSetEnumerate(ConcreteRpc.ComIdSet.Enumerate request) throws RebarException {
        Collection<String> self = comIdSets.get(request.getSelf().getId());
        return RPCResponse.newBuilder().addAllComId(self).build();
    }

    private RPCResponse handleVertexIdSetEnumerate(ConcreteRpc.VertexIdSet.Enumerate request) throws RebarException {
        Collection<Concrete.UUID> self = vertexIdSets.get(request.getSelf().getId());
        return RPCResponse.newBuilder().addAllVertexId(self).build();
    }

    private RPCResponse handleEdgeIdSetEnumerate(ConcreteRpc.EdgeIdSet.Enumerate request) throws RebarException {
        Collection<Concrete.EdgeId> self = edgeIdSets.get(request.getSelf().getId());
        return RPCResponse.newBuilder().addAllEdgeId(self).build();
    }

    // ////////////////////////////////////////////////////////////////////
    // Command Line Interface
    // ////////////////////////////////////////////////////////////////////

    public static void main(String[] args) throws Exception {
        if (args.length != 0) {
            System.err.println("USAGE: UnnamedPipeRPC");
            System.exit(-1);
        }
        // We use system.exit here (rather than just returning) to be
        // *sure* that we exit, even if some accumulo thread didn't
        // get closed (e.g., because an exception was raised).
        try {
            UnnamedPipeRPC rpc = new UnnamedPipeRPC();
            rpc.run();
            rpc.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            System.exit(0);
        }
    }

}

/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


/** Interface for reading and writing from REBAR knowledge graphs.
 */
package edu.jhu.rebar;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import edu.jhu.concrete.Concrete;
import edu.jhu.rebar.accumulo.AccumuloBackedGraph;

/** Interface for reading and writing from a "REBAR Knowledge Graph".
 * Each "REBAR Knowledge Graph" consists of a set of Vertices
 * connected by a set of Edges.
 *
 * Each graph is backed by some external shared resource (such as an
 * Accumulo table or a directory on a filesystem), and any changes
 * made to the graph are persistant.
 */
public interface Graph extends StagedDataCollection {
	public static final class Factory {
		public static Graph makeGraph(String graphName) throws RebarException {
			return new AccumuloBackedGraph(graphName, true);
		}
		public static Graph getGraph(String graphName) throws RebarException {
			return new AccumuloBackedGraph(graphName, false);
		}
		public static boolean graphExists(String graphName) throws RebarException {
			return AccumuloBackedGraph.graphExists(graphName);
		}
		public static Collection<String> list() throws RebarException {
			return AccumuloBackedGraph.listGraphs();
		}
		public static void deleteGraph(String graphName) throws RebarException {
			AccumuloBackedGraph.deleteGraph(graphName);
		}
	}

	// ======================================================================
	// Identifier Sets
	// ======================================================================

	/** Return a new identifier set containing the vertex identifiers
	 * listed in the specified file.  The file should contain one
	 * identifier per line. */
	public Collection<Concrete.UUID> readVertexIdSet(File filename) throws RebarException;

	/** Permanently associate a name with a given vertex identifier
	 * set.  This identifier set can then be looked up using the
	 * method 'lookupVertexIdSet()'. */
	public void registerVertexIdSet(String symbolicName, Collection<Concrete.UUID> idSet) throws RebarException;

	/** Look up and return a vertex identifier set that has been
	 * defined using the 'registerVertexIdSet()' method. */
	public Collection<Concrete.UUID> lookupVertexIdSet(String symbolicName) throws RebarException;

	/** Return a new identifier set containing the edge identifiers
	 * listed in the specified file.  The file should contain one
	 * identifier per line. */
	public Collection<Concrete.EdgeId> readEdgeIdSet(File filename) throws RebarException;

	/** Permanently associate a name with a given edge identifier set.
	 * This identifier set can then be looked up using the method
	 * 'lookupEdgeIdSet()'. */
	public void registerEdgeIdSet(String symbolicName, Collection<Concrete.EdgeId> idSet) throws RebarException;

	/** Look up and return an edge identifier set that has been
	 * defined using the 'registerEdgeIdSet()' method. */
	public Collection<Concrete.EdgeId> lookupEdgeIdSet(String symbolicName) throws RebarException;

	/** Get a list of the names of all symbolic vertex identifier sets
	 * that have been defined for this graph. */
	public Collection<String> getVertexIdSetNames() throws RebarException;

	/** Get a list of the names of all symbolic edge identifier sets
	 * that have been defined for this graph. */
	public Collection<String> getEdgeIdSetNames() throws RebarException;

	// ======================================================================
	// Initializer
	// ======================================================================

	/** Interface used to initialize a new graph by defining the set
	 * of vertices that it contains.
	 *
	 * (This interface could also be used to add new Vertices to
	 * an existing graph, but this should be done with care, since
	 * any stages that have been run will not contain output for these
	 * new Vertices.)
	 *
	 * Note that edges between vertices are added to the graph
	 * "lazily" (i.e., on demand) by the Graph.Writer class; they
	 * do not need to be added using an Initializer.
	 */
	interface Initializer {
		public IndexedVertex addVertex(Concrete.Vertex vertex) throws RebarException;
		public void close() throws RebarException;
	}

	/** Return a Graph.Initializer that can be used to add
	 * Vertex objects to this graph. */
	public Initializer makeInitializer() throws RebarException;

	// ======================================================================
	// Reader
	// ======================================================================

	/** Interface for "graph readers", which are used to read vertices
	 * and edges from a graph, including the output of a specified set
	 * of stages. */
	interface Reader {
		/** Read a single vertex */
		public IndexedVertex loadVertex(Concrete.UUID vertexId) throws RebarException; 

		/** Read all vertices */
		public Iterator<IndexedVertex> loadVertices() throws RebarException;

		/** Read a specified set of vertices */
		public Iterator<IndexedVertex> loadVertices(Collection<Concrete.UUID> vertexIds) throws RebarException;
		
		/** Read a single edge.  If no edge currently exists between
		 * the given pair of vertices, then a new edge will be created
		 * (but this edge will not be saved until a call is made to 
		 * Graph.Writer.saveEdge()). */
		public IndexedEdge loadEdge(Concrete.EdgeId edgeId) throws RebarException;

		public IndexedEdge loadEdge(Concrete.DirectedEdgeId directedEdgeId) throws RebarException;

		/** Read the edge that connects a pair of vertices.
		 * @see Graph#loadEdge(Concrete.EdgeId) */
		public IndexedEdge loadEdge(IndexedVertex src, IndexedVertex dst) throws RebarException;

		/** Read the edge that connects a pair of vertices, specified by Concrete.UUID.
		 * @see Graph#loadEdge(Concrete.EdgeId) */
		public IndexedEdge loadEdge(Concrete.UUID src, Concrete.UUID dst) throws RebarException;

		/** Read all edges. */
		public Iterator<IndexedEdge> loadEdges() throws RebarException;

		/** Read a specified set of edges */
		public Iterator<IndexedEdge> loadEdges(Collection<Concrete.EdgeId> edgeIds) throws RebarException;

		/** Release any resources associated with this reader. */
		public void close() throws RebarException;

		/** Return the list of input stages for this reader. */
		public Collection<Stage> getInputStages() throws RebarException;

		/** Return the graph that owns this reader. */
		Graph getGraph();
	}

	/** Return a Graph.Reader that reads vertices and edges from this
	 * graph. 
	 *
	 * @param stages A list of stages whose output should be included
	 *    in the returned Vertices and Edges.
	 */
	public Reader makeReader(Collection<Stage> stages) throws RebarException;
	public Reader makeReader(Collection<Stage> stages, boolean loadStageOwnership) throws RebarException;

	/** Return a collection of EdgeIds for edges in this graph where
	 * at least one endpoint of the edge is in the given list of
	 * vertices. */
	public Collection<Concrete.EdgeId> getEdgesWithOneVertexIn(Collection<Concrete.UUID> vertexIds) throws RebarException;

	/** Return a collection of EdgeIds for edges in this graph where
	 * both endpoints of the edge are in the given list of vertices. */
	public Collection<Concrete.EdgeId> getEdgesWithBothVerticesIn(Collection<Concrete.UUID> vertexIds) throws RebarException;

	/** Return the number of vertices in this Corpus. */
	public long getNumVertices() throws RebarException;

	// ======================================================================
	// Writer
	// ======================================================================

	/** Interface for "graph writers," which are used to add
	 * the output of a processing stage to some or all of the
	 * vertices in a graph.
	 */
	interface Writer {
		/** Save any changes that have been made to the given vertex
		 * (or do nothing if no changes have been made).  Changes can
		 * be made to an IndexedVertex using its addField() and
		 * SetField() methods, as well as various convenience methods
		 * (such as addAttribute()) that delegate to these
		 * methods. */
		public void saveVertex(IndexedVertex vertex) throws RebarException;

		/** Save any changes that have been made to the given edge
		 * (or do nothing if no changes have been made).  Changes can
		 * be made to an IndexedEdge using its addField() and
		 * SetField() methods, as well as various convenience methods
		 * (such as addAttribute()) that delegate to these
		 * methods. */
		public void saveEdge(IndexedEdge edge) throws RebarException;

		public void flush() throws RebarException;
		public void close() throws RebarException;
		public Stage getOutputStage();
	}

	/** Return a Graph.Writer that can be used to add the output
	 * of a processing stage to this graph. */
	public Writer makeWriter(Stage stage) throws RebarException;

	/** Interface used by RPC to write "diffs", i.e., monotonic
	 * changes to edges and verticess.  Each diff consists of a
	 * dictionary that maps from UUIDs of objects contained within an
	 * edge or vertex to serialized protobuf messages that should be
	 * merged into those objects.  Since diffs are merged in, they can
	 * only be used to make monotonic changes (i.e., add field values
	 * or set field values, not remove or modify them). */
	interface DiffWriter {
		public void saveVertexDiff(Concrete.UUID vertexId, Map<ProtoIndex.ModificationTarget, byte[]> changes) throws RebarException;
		// edge auto-creation.. hm!
		public void saveEdgeDiff(Concrete.EdgeId edgeId, Map<ProtoIndex.ModificationTarget, byte[]> changes) throws RebarException;
		public void flush() throws RebarException;
		public void close() throws RebarException;
	}
	public DiffWriter makeDiffWriter(Stage stage) throws RebarException;


	// ======================================================================
	// Vertex/Edge Loader Interfaces
	// ======================================================================

	public static interface VertexLoader {
		public IndexedVertex loadVertex(Concrete.UUID uuid);
	}
	public static interface EdgeLoader {
		public IndexedEdge loadEdge(Concrete.EdgeId edgeId);
	}
}

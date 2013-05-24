/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.rebar.accumulo;

import java.util.Set;
import java.util.SortedSet;

import org.apache.hadoop.io.Text;

import edu.jhu.hlt.concrete.util.ByteUtil;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;
import edu.jhu.rebar.StagedDataCollection;

/**
 * Base class for the Accumulo backed Corpus and Graph classes. This class is
 * not indended to be directly instantiated.
 */
public class AccumuloBackedStagedDataCollection implements StagedDataCollection {
    // ======================================================================
    // Constants
    // ======================================================================
    // We use a one-byte prefix for each column family type.
    protected final static byte STAGE_CF_PREFIX = 10;
    protected final static String STAGES_TABLE_SUFFIX = "_stages";

    // ======================================================================
    // Private Variables
    // ======================================================================
    protected final AccumuloStageTable stageTable;
    protected final AccumuloConnector accumuloConnector;
    protected final AccumuloSummaryTable summaryTable;
    protected final String name;
    protected final String[] tableNames;
    protected final String[] protoTableNames;
    protected final byte[] stageCFPrefixes;

    // ======================================================================
    // Constructor
    // ======================================================================

    protected AccumuloBackedStagedDataCollection(String name, boolean create, String tablePrefix, String[] tableSuffixes,
            String[] protoTableSuffixes, byte[] stageCFPrefixes) throws RebarException {
        this.name = name;
        this.accumuloConnector = new AccumuloConnector();
        if (create) {
            for (String suffix : tableSuffixes)
                accumuloConnector.createTable(tablePrefix + name + suffix);
        } else {
            for (String suffix : tableSuffixes) {
                if (!accumuloConnector.tableExists(tablePrefix + name + suffix)) {
                    throw new RebarException("Table " + tablePrefix + name + suffix + " not found; is the name \"" + name + "\" correct?");
                }
            }
        }
        this.stageTable = new AccumuloStageTable(accumuloConnector, tablePrefix + name + STAGES_TABLE_SUFFIX, this);
        this.summaryTable = new AccumuloSummaryTable(accumuloConnector, tablePrefix);
        this.tableNames = new String[tableSuffixes.length];
        for (int i = 0; i < tableSuffixes.length; i++)
            tableNames[i] = tablePrefix + name + tableSuffixes[i];
        this.protoTableNames = new String[protoTableSuffixes.length];
        for (int i = 0; i < protoTableSuffixes.length; i++)
            protoTableNames[i] = tablePrefix + name + protoTableSuffixes[i];
        this.stageCFPrefixes = stageCFPrefixes;
    }

    // ======================================================================
    // Interface Methods
    // ======================================================================

    @Override
    public void close() throws RebarException {
        summaryTable.close();
        accumuloConnector.release();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Stage getStage(String stageName, String stageVersion) throws RebarException {
        return stageTable.getStage(stageName, stageVersion);
    }

    @Override
    public Stage getStage(int stageId) throws RebarException {
        return stageTable.getStage(stageId);
    }

    @Override
    public Stage makeStage(String stageName, String stageVersion, Set<Stage> dependencies, String description, boolean deleteIfExists)
            throws RebarException {
        return stageTable.addStage(stageName, stageVersion, dependencies, description, deleteIfExists);
    }

    @Override
    public SortedSet<Stage> getStages() throws RebarException {
        return stageTable.getStages();
    }

    @Override
    public Stage getStage(String stageString) throws RebarException {
        return stageTable.getStage(stageString);
    }

    @Override
    public boolean hasStage(String stageName, String stageVersion) throws RebarException {
        return stageTable.hasStage(stageName, stageVersion);
    }

    @Override
    public void markStagePublic(Stage stage) throws RebarException {
        stageTable.markStagePublic(stage);
    }

    @Override
    public void deleteStage(Stage stage) throws RebarException {
        // delete protobuf contents of the stage:
        for (byte prefix : stageCFPrefixes) {
            Text cf = new Text(ByteUtil.fromIntWithPrefix(prefix, stage.getStageId()));
            for (String tableName : protoTableNames)
                accumuloConnector.deleteColumnFamily(tableName, cf);
        }
        // delete record of the stage:
        stageTable.deleteStage(stage);
    }
}

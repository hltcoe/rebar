/**
 * 
 */
package edu.jhu.rebar.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;

/**
 * @author max
 * 
 */
public class FileBackedCorpus implements Corpus {

    private static final Logger logger = LoggerFactory.getLogger(FileBackedCorpus.class);

    private final Path pathOnDisk;
    private final Path stagesPath;
    private final Path stageIdToStageNameFile;

    private final Connection conn;

    /**
     * 
     */
    public FileBackedCorpus(Path pathOnDisk) throws RebarException {
        this.pathOnDisk = pathOnDisk;
        this.stagesPath = this.pathOnDisk.resolve("stages");
        this.stageIdToStageNameFile = this.pathOnDisk.resolve("stages.db");
        try {
            Files.createDirectories(this.pathOnDisk);
            boolean initializeStageFile = !Files.exists(this.stageIdToStageNameFile);

            Class.forName("org.sqlite.JDBC");
            String connString = "jdbc:sqlite:" + this.stageIdToStageNameFile.toString();
            this.conn = DriverManager.getConnection(connString);
            if (initializeStageFile) {
                Statement statement = this.conn.createStatement();
                statement.executeUpdate("CREATE TABLE stages (id integer PRIMARY KEY, name string, version string, description text)");
                statement
                        .executeUpdate("CREATE TABLE dependencies (stage_id integer, dependency_id integer, " +
                        		"FOREIGN KEY(stage_id) REFERENCES stages(id))");
            }
        } catch (IOException | ClassNotFoundException | SQLException ioe) {
            throw new RebarException(ioe);
        }
    }

    public Path getPath() {
        return this.pathOnDisk;
    }

    @Override
    public String getName() {
        return this.pathOnDisk.getFileName().toString();
    }

    @Override
    public void close() throws RebarException {
        try {
            this.conn.close();
        } catch (SQLException e) {
            throw new RebarException(e);
        }
    }

    @Override
    public Stage makeStage(String stageName, String stageVersion, Set<Stage> dependencies, String description, boolean deleteIfExists)
            throws RebarException {
        try {
            if (this.hasStage(stageName, stageVersion))
                throw new RebarException("Stage: " + stageName + " already exists!");
            
            PreparedStatement ps = this.conn.prepareStatement("INSERT INTO stages VALUES (?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, null);
            ps.setString(2, stageName);
            ps.setString(3, stageVersion);
            ps.setString(4, description);
            ps.execute();
            ResultSet rs = ps.getGeneratedKeys();
            int id;
            if (rs.next())
                id = rs.getInt(1);
            else
                throw new SQLException("Creating stage failed; didn't get an id back after insert.");
            if (dependencies.size() > 0) {
                PreparedStatement depStmt = this.conn.prepareStatement("INSERT INTO dependencies VALUES (?, ?)");
                for (Stage dep : dependencies) {
                    depStmt.setInt(1, id);
                    depStmt.setInt(2, dep.getStageId());
                    depStmt.addBatch();
                }

                depStmt.executeBatch();
            }
            return new FileStage(stageName, stageVersion, id, this.pathOnDisk, dependencies, description, false);
        } catch (SQLException e) {
            throw new RebarException(e);
        }
    }

    @Override
    public void markStagePublic(Stage stage) throws RebarException {
        // TODO Auto-generated method stub

    }

    @Override
    public Stage getStage(String stageName, String stageVersion) throws RebarException {
        return this.queryStage(stageName, stageVersion);
    }

    @Override
    public Stage getStage(int stageId) throws RebarException {
        return this.queryStage(stageId);
    }

    @Override
    public Stage getStage(String stageString) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SortedSet<Stage> getStages() throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasStage(String stageName, String stageVersion) throws RebarException {
        return this.queryStageExists(stageName, stageVersion);
    }

    @Override
    public void deleteStage(Stage stage) throws RebarException {
        // TODO Auto-generated method stub

    }

    @Override
    public Collection<String> readComIdSet(File filename) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void registerComIdSet(String name, Collection<String> idSet) throws RebarException {
        // TODO Auto-generated method stub

    }

    @Override
    public Collection<String> lookupComIdSet(String name) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<String> getComIdSetNames() throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Initializer makeInitializer() throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Reader makeReader(Collection<Stage> stages) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Reader makeReader(Stage[] stages) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Reader makeReader(Stage stage) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Reader makeReader() throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Reader makeReader(Collection<Stage> stages, boolean loadStageOwnership) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Reader makeReader(Stage[] stages, boolean loadStageOwnership) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Reader makeReader(Stage stage, boolean loadStageOwnership) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Reader makeReader(boolean loadStageOwnership) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getNumCommunications() throws RebarException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Writer makeWriter(Stage stage) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DiffWriter makeDiffWriter(Stage stage) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    private int queryStageId(String stageName, String stageVersion) throws RebarException {
        PreparedStatement ps = null;
        try {
            ps = this.conn.prepareStatement("SELECT id FROM stages WHERE name = ? AND version = ?");
            ps.setString(1, stageName);
            ps.setString(2, stageVersion);
            ResultSet rs = ps.executeQuery();
            if (rs.next())
                return rs.getInt(1);
            else
                throw new SQLException("Didn't get an id for stage: " + stageName + " and version: " + stageVersion);

        } catch (SQLException ioe) {
            throw new RebarException(ioe);
        } finally {
            if (ps != null)
                try {
                    ps.close();
                } catch (SQLException e) {
                    logger.debug("Failed to close statement.", e);
                }
        }
    }
    
    private boolean queryStageExists(String stageName, String stageVersion) throws RebarException {
        PreparedStatement ps = null;
        try {
            ps = this.conn.prepareStatement("SELECT id FROM stages WHERE name = ? AND version = ?");
            ps.setString(1, stageName);
            ps.setString(2, stageVersion);
            ResultSet rs = ps.executeQuery();
            return rs.next();
        } catch (SQLException ioe) {
            throw new RebarException(ioe);
        } finally {
            if (ps != null)
                try {
                    ps.close();
                } catch (SQLException e) {
                    logger.debug("Failed to close statement.", e);
                }
        }
    }
    
    private FileStage queryStage(int stageId) throws RebarException {
        PreparedStatement ps = null;
        try {
            Set<Stage> dependencies = this.queryStageDependencies(stageId);
            ps = this.conn.prepareStatement("SELECT name, version, description FROM stages WHERE id = ?");
            ps.setInt(1, stageId);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String name = rs.getString(1);
                String vers = rs.getString(2);
                String desc = rs.getString(3);
                return new FileStage(name, vers, stageId, this.pathOnDisk, dependencies, desc, true);
            }
            else
                throw new SQLException("Didn't find id: " + stageId);

        } catch (SQLException ioe) {
            throw new RebarException(ioe);
        } finally {
            if (ps != null)
                try {
                    ps.close();
                } catch (SQLException e) {
                    logger.debug("Failed to close statement.", e);
                }
        }
    }
    
    private FileStage queryStage(String stageName, String stageVersion) throws RebarException {
        PreparedStatement ps = null;
        try {
            int stageId = this.queryStageId(stageName, stageVersion);
            Set<Stage> dependencies = this.queryStageDependencies(stageId);
            ps = this.conn.prepareStatement("SELECT description FROM stages WHERE id = ?");
            ps.setInt(1, stageId);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String desc = rs.getString(1);
                return new FileStage(stageName, stageVersion, stageId, this.pathOnDisk, dependencies, desc, true);
            }
            else
                throw new SQLException("Didn't get an id for stage: " + stageName + " and version: " + stageVersion);

        } catch (SQLException ioe) {
            throw new RebarException(ioe);
        } finally {
            if (ps != null)
                try {
                    ps.close();
                } catch (SQLException e) {
                    logger.debug("Failed to close statement.", e);
                }
        }
    }
    
    private List<Integer> queryDependencyIds (int stageId) throws RebarException {
        List<Integer> intList = new ArrayList<>();
        PreparedStatement ps = null;
        try {
            ps = this.conn.prepareStatement("SELECT dependency_id FROM dependencies WHERE stage_id = ?");
            ps.setInt(1, stageId);
            ResultSet rs = ps.executeQuery();
            while (rs.next())
                intList.add(rs.getInt(1));

            rs.close();
            return intList;
        } catch (SQLException ioe) {
            throw new RebarException(ioe);
        } finally {
            if (ps != null)
                try {
                    ps.close();
                } catch (SQLException e) {
                    logger.debug("Failed to close statement.", e);
                }
        }
    }
    
    private Set<Stage> queryStageDependencies(int stageId) throws RebarException {
        Set<Stage> stages = new TreeSet<>();
        PreparedStatement ps = null;
        try {
            ps = this.conn.prepareStatement("SELECT dependency_id " +
            		"FROM dependencies WHERE stage_id = ?");
            ps.setInt(1, stageId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int depId = rs.getInt(1);
                // recurse to find dependencies until we've satisfied them.
                Set<Stage> deps = this.queryStageDependencies(depId);
                String depName = rs.getString(2);
                String depVersion = rs.getString(3);
                String description = rs.getString(4);
                stages.add(new FileStage(depName, depVersion, depId, this.pathOnDisk, deps, description, true));
            }
            
            return stages;
        } catch (SQLException ioe) {
            throw new RebarException(ioe);
        } finally {
            if (ps != null)
                try {
                    ps.close();
                } catch (SQLException e) {
                    logger.debug("Failed to close statement.", e);
                }
        }
    }
}

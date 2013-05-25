/**
 * 
 */
package edu.jhu.rebar.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.CorpusFactory;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.util.FileUtil;

/**
 * @author max
 * 
 */
public class FileCorpusFactory implements CorpusFactory {

    private static final Logger logger = LoggerFactory.getLogger(FileCorpusFactory.class);

    private final Path pathOnDisk;
    //private final File[] corpora;
    Set<String> corporaList;

    /**
     * @throws RebarException
     * 
     */
    public FileCorpusFactory() throws RebarException {
        //this(Paths.get("/export", "common", "rebar"));
        this(Paths.get("target"));
    }

    /**
     * 
     * @param params
     * @throws RebarException
     */
    public FileCorpusFactory(String... params) throws RebarException {
        this(Paths.get("target/file_corpus_factory_test"));
    }

    /**
     * 
     * @param pathOnDisk
     * @throws RebarException
     */
    public FileCorpusFactory(Path pathOnDisk) throws RebarException {
        this.pathOnDisk = pathOnDisk;
        this.corporaList = new TreeSet<>();
        try {
            if (!Files.exists(pathOnDisk)) {
                logger.info("Creating path: " + this.pathOnDisk.toString());
                Files.createDirectories(pathOnDisk);
                this.corporaList = new TreeSet<>();
            } else {
            	DirectoryStream<Path> ds = Files.newDirectoryStream(this.pathOnDisk);
                Iterator<Path> pathIter = ds.iterator();
                while (pathIter.hasNext())
                    corporaList.add(pathIter.next().getFileName().toString());
            }
        } catch (IOException e) {
            throw new RebarException(e);
        }
    }
    
	public FileBackedCorpus initializeCorpus(String corpusName,
			Iterator<Communication> commIdIter) throws RebarException {
		if (this.corpusExists(corpusName))
			throw new RebarException("Corpus " + corpusName + " already exists. Call getCorpus() instead.");
		try {
			Path pathToCorpus = this.getPathToCorpus(corpusName);
			Connection conn = this.getConnection(corpusName);
			this.createSQLiteBackend(conn);

			Set<String> commIdSet = new TreeSet<>();
			PreparedStatement ps = conn.prepareStatement("INSERT INTO comm_ids VALUES (?, ?)");
			while (commIdIter.hasNext()) {
				Communication comm = commIdIter.next();
				String guid = comm.getGuid().getCommunicationId();
				boolean newComm = commIdSet.add(guid);
				if (newComm) {
					ps.setString(1, null);
					ps.setString(2, guid);
					ps.addBatch();
				}
			}
			
			ps.executeBatch();
			this.corporaList.add(corpusName);
			
			FileBackedCorpus fbc = 
					new FileBackedCorpus(pathToCorpus, commIdSet, conn);
			return fbc;
		} catch (SQLException e) {
			throw new RebarException(e);
		} finally {

		}
	}
	
    private Path createCorpusAndDatabasePaths(String corpusName) throws RebarException {
    	try {
        	Path corpusPath = this.pathOnDisk.resolve(corpusName);
        	if (!Files.exists(corpusPath))
        		Files.createDirectories(corpusPath);
        	Path dbFile = corpusPath.resolve(corpusName + ".db");
        	if (!Files.exists(dbFile))
        		Files.createFile(dbFile);
        	return dbFile;
    	} catch (IOException ioe) {
    		throw new RebarException(ioe);
    	}
    }
    
    private void createSQLiteBackend (Connection conn) throws RebarException {
    	try {
    	Statement statement = conn.createStatement();
    	statement.executeUpdate("CREATE TABLE stages (id integer PRIMARY KEY, name string, version string, description text)");
    	statement.executeUpdate("CREATE TABLE dependencies (stage_id integer, dependency_id integer, " +
    			"FOREIGN KEY(stage_id) REFERENCES stages(id))");
    	statement.executeUpdate("CREATE TABLE comm_ids (id integer PRIMARY KEY, guid string)");
    	statement.close();
        } catch (SQLException ioe) {
            throw new RebarException(ioe);
        }
    }
    
    private Connection getConnection(String corpusName) throws RebarException {
        try {
            Class.forName("org.sqlite.JDBC");
            Path dbFilePath = this.createCorpusAndDatabasePaths(corpusName);
            String connString = "jdbc:sqlite:" + 
            		dbFilePath.toString();
            return DriverManager.getConnection(connString);
        } catch (ClassNotFoundException e) {
            throw new RebarException(e);
        } catch (SQLException e) {
            throw new RebarException(e);
        }
    }

    @Override
    public Corpus makeCorpus(String corpusName) throws RebarException {
    	return null;
//        try {
//            if (this.corpusExists(corpusName))
//                return this.getCorpus(corpusName);
//            else {
//                if (!Files.exists(this.getPathToCorpus(corpusName)))
//                    Files.createDirectory(this.getPathToCorpus(corpusName));
//                return new FileBackedCorpus(this.getPathToCorpus(corpusName));
//            }
//        } catch (IOException e) {
//            throw new RebarException(e);
//        }
    }

    @Override
    public Corpus getCorpus(String corpusName) throws RebarException {
    	if (this.corpusExists(corpusName)) {
    		Path p = this.getPathToCorpus(corpusName);
    		return new FileBackedCorpus(p, this.getConnection(corpusName));
    	} else
            throw new RebarException("Corpus: " + corpusName + " doesn't exist. Create it first.");
    }

    @Override
    public boolean corpusExists(String corpusName) throws RebarException {
        return this.corporaList.contains(corpusName);
    }

    @Override
    public Set<String> listCorpora() throws RebarException {
        return Collections.unmodifiableSet(this.corporaList);
    }

    @Override
    public void deleteCorpus(String corpusName) throws RebarException {
    	if (this.corpusExists(corpusName)) {
    		FileUtil.deleteFolderAndSubfolders(this.getPathToCorpus(corpusName));
    		this.corporaList.remove(corpusName);
    	} else {
    		throw new RebarException("Corpus: " + corpusName + " doesn't exist; couldn't delete it.");
    	}
    }

    private Path getPathToCorpus(String corpusName) {
        return this.pathOnDisk.resolve(corpusName);
    }

}

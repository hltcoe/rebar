/**
 * 
 */
package edu.jhu.rebar.file;


import java.io.File;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.Concrete.CommunicationGUID;
import edu.jhu.hlt.concrete.io.ProtocolBufferWriter;
import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.CorpusFactory;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.config.RebarConfiguration;
import edu.jhu.rebar.util.FileUtil;

/**
 * @author max
 * 
 */
public class FileCorpusFactory implements CorpusFactory {

	public class FileCorpusFactoryInitializer {
		private final String name;
		private final Path corpusPath;
		private final Path commsPath;
		private final Path dbPath;
		private final Set<String> commIdSet;
		private final Connection conn;
		
		public FileCorpusFactoryInitializer (String name) 
		        throws RebarException {
			this.name = name;
			this.corpusPath = getPathToCorpus(name);
			this.dbPath = this.corpusPath.resolve(this.name + ".db");
			this.commsPath = this.corpusPath.resolve("communications");
			this.commIdSet = new TreeSet<>();
			this.createFoldersAndFiles();
			this.conn = FileCorpusFactory.this.getConnection(name);
			this.createSQLiteBackend();
		}
		
		private void createFoldersAndFiles() throws RebarException {
	    	try {
	        	Files.createDirectories(corpusPath);
	        	Files.createFile(dbPath);
	        	Files.createDirectory(commsPath);
	    	} catch (IOException ioe) {
	    		throw new RebarException(ioe);
	    	}
	    }
		
		private void writeCommunicationProtobufFile(Communication comm) 
	    		throws RebarException {
            try {
                String guid = comm.getGuid().getCommunicationId();
                Path fileOutPath = this.commsPath.resolve(guid + ".pb");
                Files.createFile(fileOutPath);
                File outFile = fileOutPath.toFile();
                ProtocolBufferWriter pbw = new ProtocolBufferWriter(
                        new FileOutputStream(outFile));
                pbw.write(comm);
                pbw.close();
            } catch (IOException ioe) {
                throw new RebarException(ioe);
            }
	    }
		
		private void createSQLiteBackend () throws RebarException {
            try {
                Statement statement = this.conn.createStatement();
                statement.executeUpdate("CREATE TABLE stages "
                        + "(id integer PRIMARY KEY, "
                        + "name string, version string, description text)");
                statement.executeUpdate("CREATE TABLE dependencies "
                        + "(stage_id integer, " + "dependency_id integer, "
                        + "FOREIGN KEY(stage_id) REFERENCES stages(id))");
                statement.executeUpdate("CREATE TABLE comm_ids "
                        + "(id integer PRIMARY KEY, " + "guid string)");
                statement.executeUpdate("CREATE TABLE '" + "stage_mods"
                        + "' (stage_id integer, " + "comm_id string, "
                        + "target blob, " + "mods blob, "
                        + "FOREIGN KEY(stage_id) REFERENCES stages(id))");
                statement.close();
            } catch (SQLException ioe) {
                throw new RebarException(ioe);
            }
	    }
		
		public void ingest(Communication comm) throws RebarException {
		    String guid = comm.getGuid().getCommunicationId();
            boolean newComm = commIdSet.add(guid);
            if (newComm) {
                CommunicationGUID corpusNameGuid = CommunicationGUID.newBuilder(comm.getGuid())
                        .setCorpusName(this.name)
                        .build();
                comm = Communication.newBuilder(comm)
                        .setGuid(corpusNameGuid)
                        .build();
                this.writeCommunicationProtobufFile(comm);
            }
		}
		
		public void ingest(Iterator<Communication> commIter) throws RebarException {
		    while (commIter.hasNext()) {
                Communication comm = commIter.next();
                this.ingest(comm);
            }
		}
		
        public FileBackedCorpus seal() throws RebarException {
            try {
                for (String id : this.commIdSet) {
                PreparedStatement ps = conn
                        .prepareStatement("INSERT INTO comm_ids VALUES (?, ?)");
                    ps.setString(1, null);
                    ps.setString(2, id);
                    ps.addBatch();
                    ps.executeBatch();
                }
                
                corporaList.add(this.name);
                return new FileBackedCorpus(this.corpusPath, 
                        this.commIdSet, this.conn);
            } catch (SQLException e) {
                throw new RebarException(e);
            }
        }
	}
	
    private final Path pathOnDisk;
    Set<String> corporaList;

    /**
     * @throws RebarException
     * 
     */
    public FileCorpusFactory() throws RebarException {
        this(RebarConfiguration.getFileCorpusDirectory());
    }

    /**
     * 
     * @param params
     * @throws RebarException
     */
    public FileCorpusFactory(String... params) throws RebarException {
        this(Paths.get(params[0]));
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
		FileCorpusFactoryInitializer init = 
		        new FileCorpusFactoryInitializer(corpusName);
		init.ingest(commIdIter);
		FileBackedCorpus fbc = init.seal();
		return fbc;
	}
	
	public FileBackedCorpus initializeCorpus(String corpusName,
            Collection<Communication> commColl) throws RebarException {
        return this.initializeCorpus(corpusName, commColl.iterator());
    }
    
    private Connection getConnection(String corpusName) throws RebarException {
        try {
            Class.forName("org.sqlite.JDBC");
            String connString = "jdbc:sqlite:" + 
            	this.getPathToCorpus(corpusName)
            		.resolve(corpusName + ".db")
            		.toString();
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

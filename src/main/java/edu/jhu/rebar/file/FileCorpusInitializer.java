/**
 * 
 */
package edu.jhu.rebar.file;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.TreeSet;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.io.ProtocolBufferWriter;
import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.IndexedCommunication;
import edu.jhu.rebar.ProtoIndex;
import edu.jhu.rebar.RebarException;

/**
 * @author max
 *
 */
public class FileCorpusInitializer
            implements Corpus.Initializer {

    private final Path path;
    private final Connection conn;
    private final Set<String> existingGuidSet = new TreeSet<>();
    private final Set<String> newGuidSet = new TreeSet<>();
    
    /**
     * 
     */
    public FileCorpusInitializer(Path path, Connection conn) throws RebarException {
        this.path = path;
        this.conn = conn;
        
        try {
            PreparedStatement ps = this.conn.prepareStatement("SELECT guid FROM comm_ids");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                this.existingGuidSet.add(rs.getString(1));
            }
            
            rs.close();
            ps.close();
        } catch (SQLException e) {
            throw new RebarException(e);
        }
    }

    @Override
    public IndexedCommunication addCommunication(Communication comm) throws RebarException {
        try {
            String guid = comm.getGuid().getCommunicationId();
            if (this.communicationExists(guid))
                throw new RebarException("Comm GUID: " + guid + " already exists in this corpus.");
            this.newGuidSet.add(guid);
            ProtocolBufferWriter pbw = new ProtocolBufferWriter(new FileOutputStream(this.path.toFile()));
            pbw.write(comm);
            pbw.close();
            ProtoIndex pi = new ProtoIndex(comm);
            return new IndexedCommunication(comm, pi, null);
        } catch (IOException e) {
            throw new RebarException(e);
        } finally {
            
        }
    }

    @Override
    public boolean communicationExists(String commId) throws RebarException {
        return this.newGuidSet.contains(commId) || this.existingGuidSet.contains(commId);
    }

    @Override
    public void close() throws RebarException {
        try {
            PreparedStatement ps = this.conn.prepareStatement("INSERT INTO comm_ids VALUES (?, ?)");
            for (String s : this.newGuidSet) {
                ps.setString(1, null);
                ps.setString(2, s);
                ps.addBatch();
            }
            
            ps.executeBatch();
            ps.close();
            
            this.conn.close();
        } catch (SQLException e) {
            throw new RebarException(e);
        }
    }
}

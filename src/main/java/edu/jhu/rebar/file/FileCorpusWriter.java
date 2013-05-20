/**
 * 
 */
package edu.jhu.rebar.file;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import edu.jhu.concrete.Concrete.CommunicationGUID;
import edu.jhu.concrete.io.ProtocolBufferWriter;
import edu.jhu.rebar.Corpus.Writer;
import edu.jhu.rebar.IndexedCommunication;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;

/**
 * @author max
 *
 */
public class FileCorpusWriter implements Writer {

    protected final Path path;
    protected final FileStage stage;
    
    /**
     * @throws FileNotFoundException 
     * 
     */
    public FileCorpusWriter(FileStage stage)  {
        this.stage = stage;
        this.path = this.stage.getPath();
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus.Writer#saveCommunication(edu.jhu.rebar.IndexedCommunication)
     */
    @Override
    public void saveCommunication(IndexedCommunication comm) throws RebarException {
        try {
            CommunicationGUID guid = comm.getGuid();
            Path outputPath = this.path.resolve(guid.getCommunicationId() + ".pb");
            ProtocolBufferWriter pbw = new ProtocolBufferWriter(new FileOutputStream(outputPath.toFile()));
            pbw.write(comm.getProto());
            pbw.close();
        } catch (IOException e) {
            throw new RebarException(e);
        }
    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus.Writer#flush()
     */
    @Override
    public void flush() throws RebarException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus.Writer#close()
     */
    @Override
    public void close() throws RebarException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see edu.jhu.rebar.Corpus.Writer#getOutputStage()
     */
    @Override
    public Stage getOutputStage() {
        return this.stage;
    }

}

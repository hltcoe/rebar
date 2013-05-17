/**
 * 
 */
package edu.jhu.rebar.file;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.SortedSet;

import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;

/**
 * @author max
 *
 */
public class FileBackedCorpus implements Corpus {

    private final Path pathOnDisk;
    
    /**
     * 
     */
    public FileBackedCorpus(Path pathOnDisk) {
        this.pathOnDisk = pathOnDisk;
    }

    @Override
    public String getName() {
        return this.pathOnDisk.getFileName().toString();
    }

    @Override
    public void close() throws RebarException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Stage makeStage(String stageName, String stageVersion, Collection<Stage> dependencies, String description, boolean deleteIfExists)
            throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void markStagePublic(Stage stage) throws RebarException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Stage getStage(String stageName, String stageVersion) throws RebarException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Stage getStage(int stageId) throws RebarException {
        // TODO Auto-generated method stub
        return null;
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
        // TODO Auto-generated method stub
        return false;
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

}

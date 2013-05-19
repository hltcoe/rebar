/**
 * 
 */
package edu.jhu.rebar.file;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedSet;

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
    
    private final File stageIdToStageNameFile;
    
    /**
     * 
     */
    public FileBackedCorpus(Path pathOnDisk) throws RebarException {
        this.pathOnDisk = pathOnDisk;
        this.stagesPath = this.pathOnDisk.resolve("stages");
        this.stageIdToStageNameFile = this.pathOnDisk.resolve("stageIdToStageNameFile.txt").toFile();
        try {
            Files.createDirectories(this.pathOnDisk);
            if (!this.stageIdToStageNameFile.exists())
                this.stageIdToStageNameFile.createNewFile();
        } catch (IOException ioe) {
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
        // TODO Auto-generated method stub
    }

    @Override
    public Stage makeStage(String stageName, String stageVersion, Set<Stage> dependencies, String description, boolean deleteIfExists)
            throws RebarException {
        int nextStageNumber = this.getNextStageId(stageName, stageVersion);
        return new FileStage(stageName, stageVersion, nextStageNumber, this.pathOnDisk, dependencies, description, false);
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
    
    /**
     * You *MUST* call this method to get a new stage ID. 
     * 
     * @return
     */
    private int getNextStageId(String stageName, String stageVersion) throws RebarException {
        try {
            FileInputStream in = new FileInputStream(this.stageIdToStageNameFile);
            logger.debug("Trying to get lock...");
            FileLock lock = in.getChannel().lock(0, Long.MAX_VALUE, true);
            logger.debug("Got lock.");
            Scanner sc = new Scanner(in, "UTF-8");
            try {
                int numLines = 0;
                while (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    if (line.contains(stageName) && line.contains(stageVersion))
                        throw new RebarException("This stage:version already exists. It has stage ID: " + line.split("\\s")[0]);
                    numLines++;
                }
                
                int stageNumToWrite = numLines++;
                BufferedWriter bw = new BufferedWriter(new FileWriter(this.stageIdToStageNameFile, true));
                bw.write(stageNumToWrite + "\t" + stageName + "\t" + stageVersion + "\n");
                bw.close();
                
                return stageNumToWrite;
            } finally {
                logger.debug("Releasing lock...");
                lock.release();
                logger.debug("Lock released.");
                sc.close();
                in.close();
            }
        } catch (IOException  ioe) {
            throw new RebarException(ioe);
        } finally {
            
        }
    }
    
    private int queryStageId(String stageName, String stageVersion) throws RebarException {
        try {
            FileInputStream in = new FileInputStream(this.stageIdToStageNameFile);
            logger.debug("Trying to get lock...");
            FileLock lock = in.getChannel().lock(0, Long.MAX_VALUE, true);
            logger.debug("Got lock.");
            Scanner sc = new Scanner(in, "UTF-8");
            try {
                int numLines = 0;
                while (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    if (line.contains(stageName) && line.contains(stageVersion))
                        throw new RebarException("This stage:version already exists. It has stage ID: " + line.split("\\s")[0]);
                    numLines++;
                }
                
                int stageNumToWrite = numLines++;
                BufferedWriter bw = new BufferedWriter(new FileWriter(this.stageIdToStageNameFile, true));
                bw.write(stageNumToWrite + "\t" + stageName + "\t" + stageVersion + "\n");
                bw.close();
                
                return stageNumToWrite;
            } finally {
                logger.debug("Releasing lock...");
                lock.release();
                logger.debug("Lock released.");
                sc.close();
                in.close();
            }
        } catch (IOException  ioe) {
            throw new RebarException(ioe);
        } finally {
            
        }
    }
}

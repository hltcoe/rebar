/**
 * 
 */
package edu.jhu.rebar.file;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        this(Paths.get("target"));
    }

    /**
     * 
     * @param pathOnDisk
     * @throws RebarException
     */
    public FileCorpusFactory(Path pathOnDisk) throws RebarException {
        this.pathOnDisk = pathOnDisk;
        try {
            if (!Files.exists(pathOnDisk)) {
                logger.info("Creating path: " + this.pathOnDisk.toString());
                Files.createDirectories(pathOnDisk);
            }
        } catch (IOException e) {
            throw new RebarException(e);
        }
    }

    @Override
    public Corpus makeCorpus(String corpusName) throws RebarException {
        try {
            if (this.corpusExists(corpusName))
                return this.getCorpus(corpusName);
            else {
                if (!Files.exists(this.getPathToCorpus(corpusName)))
                    Files.createDirectory(this.getPathToCorpus(corpusName));
                return new FileBackedCorpus(this.getPathToCorpus(corpusName));

            }
        } catch (IOException e) {
            throw new RebarException(e);
        }
    }

    @Override
    public Corpus getCorpus(String corpusName) throws RebarException {
        if (this.corpusExists(corpusName))
            return new FileBackedCorpus(this.getPathToCorpus(corpusName));
        else
            return this.makeCorpus(corpusName);
    }

    @Override
    public boolean corpusExists(String corpusName) throws RebarException {
        return Files.exists(this.getPathToCorpus(corpusName));
    }

    @Override
    public Set<String> listCorpora() throws RebarException {
        try {
            Set<String> corporaList = new TreeSet<>();
            DirectoryStream<Path> ds = Files.newDirectoryStream(this.pathOnDisk);
            Iterator<Path> pathIter = ds.iterator();
            while (pathIter.hasNext())
                corporaList.add(pathIter.next().getFileName().toString());
            return corporaList;
        } catch (IOException e) {
            throw new RebarException(e);
        }
    }

    @Override
    public void deleteCorpus(String corpusName) throws RebarException {
        FileUtil.deleteFolderAndSubfolders(this.getPathToCorpus(corpusName));
    }

    private Path getPathToCorpus(String corpusName) {
        return this.pathOnDisk.resolve(corpusName);
    }

}

/**
 * 
 */
package edu.jhu.rebar.examples;

import java.nio.file.Path;
import java.util.Set;
import java.util.SortedSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.rebar.Corpus;
import edu.jhu.rebar.RebarException;
import edu.jhu.rebar.Stage;
import edu.jhu.rebar.config.RebarConfiguration;
import edu.jhu.rebar.file.FileCorpusFactory;

/**
 * @author max
 *
 */
public class ListCorpora {
    
    private static final Logger logger = LoggerFactory
            .getLogger(ListCorpora.class);
    
    private final FileCorpusFactory fcf;
    
    /**
     * @throws RebarException 
     * 
     */
    public ListCorpora(Path path) throws RebarException {
        this.fcf = new FileCorpusFactory(path);
    }
    
    public void list() throws RebarException {
        Set<String> corpora = this.fcf.listCorpora();
        for (String corpusName : corpora) {
            Corpus c = this.fcf.getCorpus(corpusName);
            logger.info("Corpus: " + c.getName());
            SortedSet<Stage> stages = c.getStages();
            for (Stage s : stages) {
                logger.info("\tStage: " + s.getStageName());
                logger.info("\tVersion: " + s.getStageVersion());
            }
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws RebarException {
        ListCorpora lc = 
                new ListCorpora(RebarConfiguration.getFileCorpusDirectory());
        lc.list();
    }
}

/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.hlt.rebar.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import edu.jhu.hlt.rebar.Corpus;
import edu.jhu.hlt.rebar.CorpusFactory;
import edu.jhu.hlt.rebar.IndexedCommunication;
import edu.jhu.hlt.rebar.RebarBackends;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Stage;

public class TwitterTest {

    public static void main(String[] args) throws RebarException {
        CorpusFactory cf = RebarBackends.ACCUMULO.getCorpusFactory();
        Corpus corpus = cf.getCorpus("spanish_twitter_mitre");
        ArrayList<Stage> stages = new ArrayList<Stage>();
        stages.add(corpus.getStage("tweet_info", "1.0"));
        read(corpus, stages);
        summarize(corpus);
        corpus.close();
    }

    public static void summarize(Corpus corpus) throws RebarException {
        Collection<Stage> stages = corpus.getStages();
        System.err.println("================== CORPUS " + corpus.getName() + " ================");
        System.err.println("  Stages:");
        for (Stage stage : stages) {
            System.err.println("    " + stage.getStageId() + ") " + stage.getStageName() + " [" + stage.getStageVersion() + "]");
        }
        System.err.println("  Subsets:");
        for (String name : corpus.getComIdSetNames()) {
            Collection<String> idset = corpus.lookupComIdSet(name);
            System.err.println("    * " + name + " (" + idset.size() + " ids)");
        }
    }

    public static void read(Corpus corpus, Collection<Stage> stages) throws RebarException {
        Collection<String> subset = Arrays.asList(new String[] { "doc-3", "doc-9" });
        corpus.registerComIdSet("my-ids", subset);

        System.err.println("Reading...");
        Corpus.Reader reader = corpus.makeReader(stages);
        subset = corpus.lookupComIdSet("all");
        Iterator<IndexedCommunication> readIter = reader.loadCommunications(subset);
        while (readIter.hasNext()) {
            IndexedCommunication com = readIter.next();
            System.err.println("===================\n" + com);
        }
        reader.close();
        System.err.println("All done");
    }

}

/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;

import com.maxjthomas.dumpster.AnnotationException;
import com.maxjthomas.dumpster.Annotator;
import com.maxjthomas.dumpster.Document;
import com.maxjthomas.dumpster.LangId;
import com.maxjthomas.dumpster.Stage;

import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 * 
 */
public class RebarAnnotator extends AbstractAccumuloClient implements AutoCloseable, Annotator.Iface {

  private final AccumuloStageHandler ash;
  private static final Map<String, Set<String>> stageNameToIngestedIdSetMap;
  static {
    stageNameToIngestedIdSetMap = new HashMap<>();
  }

  /**
   * @throws RebarException
   * 
   */
  public RebarAnnotator() throws RebarException {
    this(AbstractAccumuloClient.getConnector());
  }

  public RebarAnnotator(Connector conn) throws RebarException {
    super(conn);
    this.ash = new AccumuloStageHandler(this.conn);
    for (Stage s : this.ash.getStagesInternal()) {
      String name = s.name;
      Set<String> annotatedIds = this.ash.getAnnotatedDocumentIds(s);
      stageNameToIngestedIdSetMap.put(name, annotatedIds);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.jhu.hlt.rebar.accumulo.AbstractAccumuloClient#flush()
   */
  @Override
  public void flush() throws RebarException {
    try {
      this.bw.flush();
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public void close() throws Exception {
    this.bw.close();
  }

  @Override
  public void addLanguageId(Document document, Stage stage, LangId lid) throws AnnotationException, TException {
    // the stage may not exist - if not, silently create.
    if (!this.ash.stageExists(stage.name)) {
      this.ash.createStage(stage);
    }

    Set<String> ingestedIds;
    try {
      // if this stage was created after this class was loaded in CL,
      // we will need to update our cache.
      if (!stageNameToIngestedIdSetMap.containsKey(stage.name)) {
        ingestedIds = this.ash.getAnnotatedDocumentIds(stage);
        stageNameToIngestedIdSetMap.put(stage.name, ingestedIds);
      } else {
        // given the stage and document, it might have been previously annotated. check to see.
        ingestedIds = stageNameToIngestedIdSetMap.get(stage.name);
        if (ingestedIds.contains(document.id))
          throw new TException("Document: " + document.id + " has been annotated with stage: " + stage.name + " previously.");
      }

      byte[] lidBytes = this.serializer.serialize(lid);
      final Mutation m = new Mutation(document.id);
      m.put(Constants.DOCUMENT_ANNOTATION_COLF, stage.name, new Value(lidBytes));
      this.bw.addMutation(m);
      this.ash.addAnnotatedDocument(stage, document);
      ingestedIds.add(document.id);
    } catch (MutationsRejectedException | RebarException e) {
      throw new TException(e);
    }
  }
}
